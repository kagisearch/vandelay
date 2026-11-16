/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

mod import;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::mem;

use serde_json::{Map, Value, json};

use self::import::SingleImport;
use super::blobpool::UploadPool;
use super::target::{ClientId, Maps, TargetJson};
use super::uploader::Uploader;
use super::{Exporter, Plan};
use crate::archive::{BlobId, LocalId, Store};
use crate::console::ByteSize;
use crate::error::Error;
use crate::jmap::error::JmapError;
use crate::jmap::request::Request;
use crate::jmap::session::Limits;
use crate::jmap::wire::JmapId;
use crate::sync::TypeCounts;
use crate::sync::jmap_map::EmailRow;
use crate::sync::keys::{EmailIndex, EmailKey};
use crate::types::ObjectType;

const EMAIL_MIME: &str = "message/rfc822";
const EMAIL_PARSE_MAX_ITEMS: usize = 10;
const EMAIL_KEY_PROPERTIES: [&str; 5] = ["messageId", "from", "subject", "sentAt", "to"];

impl EmailIndex {
    fn from_target(v: &Value) -> EmailIndex {
        let addresses = |k: &str| -> Vec<String> {
            v.get(k)
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.get("email").and_then(Value::as_str).map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default()
        };
        let mids: Vec<String> = v
            .get("messageId")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        EmailIndex::new(
            &mids,
            &addresses("from"),
            v.get("subject").and_then(Value::as_str).unwrap_or(""),
            v.get("sentAt").and_then(Value::as_str).unwrap_or(""),
            &addresses("to"),
        )
    }
}

impl Exporter<'_> {
    pub(super) fn reconcile_emails(
        &self,
        maps: &Maps,
        counts: &mut TypeCounts,
    ) -> Result<Plan, Error> {
        let target_keys = self.email_target_keys()?;
        let mut batch = EmailBatch {
            exporter: self,
            uploader: Uploader::new(self),
            pool: (!self.is_dry_run()).then(|| UploadPool::new(self, EMAIL_MIME)),
            maps,
            counts,
            target_keys: &target_keys,
            size: EmailBatch::initial_size(self.account.limits()),
        };
        let mut after = None;
        // Whole import batches per page, so a page does not end in a short
        // batch.
        let page_rows = |size: usize| size * (EMAIL_LOCAL_PAGE / size).max(1);
        while let Some(page) = email_local_page(self.store, after, page_rows(batch.size))? {
            after = page.last().map(|(id, _)| *id);
            let mut unmatched: Vec<&(LocalId, EmailRow)> = Vec::with_capacity(page.len());
            for entry in &page {
                let key = EmailIndex::from_json(&entry.1.message_match).key();
                if target_keys.contains(&key.hash()) {
                    batch.counts.skipped += 1;
                } else {
                    unmatched.push(entry);
                }
            }
            let mut rest = unmatched.as_slice();
            while !rest.is_empty() {
                let (chunk, tail) = rest.split_at(batch.size.min(rest.len()));
                batch.export_chunk(chunk)?;
                rest = tail;
            }
        }
        Ok(Plan::default())
    }

    /// The account's emails as hashes of their dedup keys. Fetched one
    /// `Email/get` page at a time and reduced to 16 bytes each, so memory
    /// stays small for large accounts. Emails without a Message-ID are
    /// fetched again with the fallback fields, page by page.
    fn email_target_keys(&self) -> Result<HashSet<[u8; 16]>, Error> {
        let ty = ObjectType::Email;
        let ids = self.account.query_all_ids(ty.jmap_name())?;
        let mut keys = HashSet::with_capacity(ids.len());
        let page =
            usize::try_from(self.account.limits().max_objects_in_get.max(1)).unwrap_or(usize::MAX);
        for chunk in ids.chunks(page) {
            let got = self
                .account
                .get::<Value>(ty.jmap_name(), chunk, Some(&["messageId"]))?;
            let mut fallback: HashMap<String, [u8; 16]> = HashMap::new();
            for v in &got.list {
                let key = EmailIndex::from_target(v).key();
                match (&key, TargetJson(v).id()) {
                    (EmailKey::Fallback(_), Some(id)) => {
                        fallback.insert(id.to_owned(), key.hash());
                    }
                    _ => {
                        keys.insert(key.hash());
                    }
                }
            }
            if fallback.is_empty() {
                continue;
            }
            let fallback_ids: Vec<JmapId> = fallback.keys().cloned().map(JmapId).collect();
            let full = self.account.get::<Value>(
                ty.jmap_name(),
                &fallback_ids,
                Some(&EMAIL_KEY_PROPERTIES),
            )?;
            for v in &full.list {
                if let Some(id) = TargetJson(v).id() {
                    fallback.remove(id);
                }
                keys.insert(EmailIndex::from_target(v).key().hash());
            }
            // An email gone between the two reads keeps the key its first
            // read gave.
            keys.extend(fallback.into_values());
        }
        Ok(keys)
    }
}

/// About how many archive rows are read per page (rounded to whole import
/// batches), so the export holds one page of rows rather than the whole
/// archive.
const EMAIL_LOCAL_PAGE: usize = 1024;

/// The archive's emails after `after` (by id), at most `limit` of them; None
/// once there are no more.
fn email_local_page(
    store: Store<'_>,
    after: Option<LocalId>,
    limit: usize,
) -> Result<Option<Vec<(LocalId, EmailRow)>>, Error> {
    let sql = format!("{} WHERE id > ?1 ORDER BY id LIMIT ?2", EmailRow::SELECT);
    let mut stmt = store
        .connection()
        .prepare_cached(&sql)
        .map_err(Error::partial)?;
    let after = after.map_or(i64::MIN, LocalId::get);
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let rows = stmt
        .query_map(rusqlite::params![after, limit], |row| {
            Ok((row.get(0)?, EmailRow::from_row(row)))
        })
        .map_err(Error::partial)?;
    let rows: Vec<_> = rows.collect::<Result<_, _>>().map_err(Error::partial)?;
    let mut out = Vec::with_capacity(rows.len());
    for (id, email) in rows {
        out.push((id, email?));
    }
    Ok((!out.is_empty()).then_some(out))
}

struct Pending<'r> {
    cid: ClientId,
    row: &'r EmailRow,
    mids: Map<String, Value>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Attempt {
    First,
    Reupload,
    LastReupload,
}

impl Attempt {
    fn suffix(self) -> &'static str {
        match self {
            Attempt::First => "",
            Attempt::Reupload | Attempt::LastReupload => " after blob re-upload",
        }
    }

    fn upload_verb(self) -> &'static str {
        match self {
            Attempt::First => "blob upload",
            Attempt::Reupload | Attempt::LastReupload => "blob re-upload",
        }
    }
}

struct BlobHint {
    mid: Option<String>,
    len: Option<u64>,
}

impl fmt::Display for BlobHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.mid {
            Some(mid) => write!(f, "message-id <{mid}>")?,
            None => f.write_str("no message-id")?,
        }
        if let Some(len) = self.len {
            write!(f, ", {}", ByteSize(len))?;
        }
        Ok(())
    }
}

struct SizeNote<'e>(&'e JmapError);

impl fmt::Display for SizeNote<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if matches!(
            self.0,
            JmapError::RequestTooLarge | JmapError::SingleObjectTooLarge(_)
        ) {
            f.write_str("; exceeds the target server size limit, so this message is skipped and re-running will not migrate it")?;
        }
        Ok(())
    }
}

struct EmailBatch<'e, 'a> {
    exporter: &'e Exporter<'a>,
    uploader: Uploader<'a>,
    pool: Option<UploadPool>,
    maps: &'e Maps,
    counts: &'e mut TypeCounts,
    target_keys: &'e HashSet<[u8; 16]>,
    size: usize,
}

impl<'r> EmailBatch<'_, '_> {
    fn initial_size(limits: &Limits) -> usize {
        usize::try_from(limits.max_objects_in_set.max(1)).unwrap_or(usize::MAX)
    }

    fn halve(previous: usize) -> usize {
        (previous / 2).max(1)
    }

    fn hint(&self, row: &EmailRow) -> BlobHint {
        BlobHint {
            mid: EmailIndex::from_json(&row.message_match)
                .mids
                .into_iter()
                .next(),
            len: self.uploader.len(row.blob),
        }
    }

    fn mailbox_ids(&self, row: &EmailRow) -> Option<Map<String, Value>> {
        let mut mids = Map::new();
        for mailbox in row.mailboxes.as_slice() {
            let target = self.maps.target(ObjectType::Mailbox, *mailbox)?;
            mids.insert(target.0, Value::Bool(true));
        }
        Some(mids)
    }

    fn export_chunk(&mut self, chunk: &[&'r (LocalId, EmailRow)]) -> Result<(), Error> {
        let mut pending: Vec<Pending<'r>> = Vec::with_capacity(chunk.len());
        for (local, row) in chunk.iter().copied() {
            let cid = ClientId::email(*local);
            match self.mailbox_ids(row) {
                Some(mids) => pending.push(Pending { cid, row, mids }),
                None => {
                    self.exporter.logger.warn(format_args!(
                        "Email/import {cid} ({}) skipped: mailbox not on target",
                        self.hint(row)
                    ));
                    self.counts.failed += 1;
                }
            }
        }
        let mut size = pending.len();
        let mut retry = self.import(pending, Attempt::First)?;
        if !retry.is_empty() {
            self.size = EmailBatch::halve(self.size);
        }
        while !retry.is_empty() {
            size = EmailBatch::halve(size);
            let attempt = if size == 1 {
                Attempt::LastReupload
            } else {
                Attempt::Reupload
            };
            for p in &retry {
                self.uploader.invalidate(p.row.blob);
            }
            let mut again = Vec::new();
            while !retry.is_empty() {
                let tail = retry.split_off(size.min(retry.len()));
                let head = mem::replace(&mut retry, tail);
                again.extend(self.import(head, attempt)?);
            }
            retry = again;
        }
        Ok(())
    }

    fn import(
        &mut self,
        pending: Vec<Pending<'r>>,
        attempt: Attempt,
    ) -> Result<Vec<Pending<'r>>, Error> {
        let logger = self.exporter.logger;
        let mut failed = self.upload(&pending);
        let on_target = match attempt {
            Attempt::First => self.parsed_on_target(&pending, &failed)?,
            Attempt::Reupload | Attempt::LastReupload => HashSet::new(),
        };
        let mut ready: Vec<Pending<'r>> = Vec::with_capacity(pending.len());
        let mut items: Vec<(ClientId, Value)> = Vec::with_capacity(pending.len());
        for p in pending {
            let blob = match failed.remove(&p.row.blob) {
                Some(e) => Err(e),
                None => self
                    .uploader
                    .cached(p.row.blob)
                    .map(|b| b.0.clone())
                    .ok_or_else(|| JmapError::malformed("blob upload did not complete")),
            };
            let blob = match blob {
                Ok(b) => b,
                Err(e) => {
                    logger.warn(format_args!(
                        "Email/import {} ({}) {} failed: {e}{}",
                        p.cid,
                        self.hint(p.row),
                        attempt.upload_verb(),
                        SizeNote(&e)
                    ));
                    self.counts.failed += 1;
                    continue;
                }
            };
            if on_target.contains(&blob) {
                self.counts.skipped += 1;
                continue;
            }
            if self.exporter.is_dry_run() {
                self.counts.created += 1;
                continue;
            }
            let keywords: Map<String, Value> = p
                .row
                .keywords
                .iter()
                .map(|k| (k.clone(), Value::Bool(true)))
                .collect();
            let item = json!({
                "blobId": blob,
                "mailboxIds": Value::Object(p.mids.clone()),
                "keywords": Value::Object(keywords),
                "receivedAt": p.row.received_at,
            });
            items.push((p.cid.clone(), item));
            ready.push(p);
        }
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(items.len());
        if let Err(e) = self.exporter.send_import(&items, &mut results) {
            let mapped = Error::from(e);
            if mapped.aborts_run() {
                return Err(mapped);
            }
            for p in ready.iter().skip(results.len()) {
                logger.warn(format_args!(
                    "Email/import {} ({}) send failed{}: {mapped}",
                    p.cid,
                    self.hint(p.row),
                    attempt.suffix()
                ));
                self.counts.failed += 1;
            }
            ready.truncate(results.len());
        }

        let mut retry = Vec::new();
        for (p, res) in ready.into_iter().zip(results) {
            match res {
                Ok(SingleImport::Created) => self.counts.created += 1,
                Ok(SingleImport::Skipped) => self.counts.skipped += 1,
                Ok(SingleImport::NotCreated { error_type, .. })
                    if attempt != Attempt::LastReupload && error_type == "blobNotFound" =>
                {
                    retry.push(p);
                }
                Ok(SingleImport::NotCreated { detail, .. }) => {
                    logger.warn(format_args!(
                        "Email/import {} ({}) failed{}: {detail}",
                        p.cid,
                        self.hint(p.row),
                        attempt.suffix()
                    ));
                    self.counts.failed += 1;
                }
                Err(e) => {
                    logger.warn(format_args!(
                        "Email/import {} ({}) send failed{}: {e}{}",
                        p.cid,
                        self.hint(p.row),
                        attempt.suffix(),
                        SizeNote(&e)
                    ));
                    self.counts.failed += 1;
                }
            }
        }
        Ok(retry)
    }

    fn parsed_on_target(
        &self,
        pending: &[Pending<'_>],
        failed: &HashMap<BlobId, JmapError>,
    ) -> Result<HashSet<String>, Error> {
        let mut found = HashSet::new();
        if self.exporter.is_dry_run() || self.target_keys.is_empty() {
            return Ok(found);
        }
        let account = self.exporter.account;
        let blobs: Vec<&str> = pending
            .iter()
            .filter(|p| !failed.contains_key(&p.row.blob))
            .filter_map(|p| self.uploader.cached(p.row.blob))
            .map(|b| b.0.as_str())
            .collect();
        let calls = usize::try_from(account.limits().max_calls_in_request.max(1)).unwrap_or(1);
        for group in blobs.chunks(EMAIL_PARSE_MAX_ITEMS.saturating_mul(calls)) {
            let mut req = Request::new();
            for (n, chunk) in group.chunks(EMAIL_PARSE_MAX_ITEMS).enumerate() {
                req.call(
                    "Email/parse",
                    json!({
                        "accountId": account.account_id(),
                        "blobIds": chunk,
                        "properties": EMAIL_KEY_PROPERTIES,
                    }),
                    format!("p{n}"),
                );
            }
            let resp = match account.send(&req) {
                Ok(resp) => resp,
                Err(e) => {
                    let mapped = Error::from(e);
                    if mapped.aborts_run() {
                        return Err(mapped);
                    }
                    self.exporter.logger.warn(format_args!(
                        "Email/parse failed, importing without a target duplicate check: {mapped}"
                    ));
                    return Ok(found);
                }
            };
            for mr in &resp.method_responses {
                if let Err(e) = mr.check_error() {
                    self.exporter.logger.warn(format_args!(
                        "Email/parse failed, importing without a target duplicate check: {e}"
                    ));
                    continue;
                }
                let Some(parsed) = mr.args.get("parsed").and_then(Value::as_object) else {
                    continue;
                };
                found.extend(
                    parsed
                        .iter()
                        .filter(|(_, email)| {
                            self.target_keys
                                .contains(&EmailIndex::from_target(email).key().hash())
                        })
                        .map(|(blob, _)| blob.clone()),
                );
            }
        }
        Ok(found)
    }

    fn upload(&mut self, pending: &[Pending<'_>]) -> HashMap<BlobId, JmapError> {
        let Some(pool) = &self.pool else {
            let mut failed = HashMap::new();
            for p in pending {
                if let Err(e) = self.uploader.upload(p.row.blob, EMAIL_MIME) {
                    failed.insert(p.row.blob, e);
                }
            }
            return failed;
        };
        let mut seen: HashSet<BlobId> = HashSet::with_capacity(pending.len());
        let mut queue: Vec<BlobId> = Vec::with_capacity(pending.len());
        for p in pending {
            let blob = p.row.blob;
            if self.uploader.cached(blob).is_none() && seen.insert(blob) {
                queue.push(blob);
            }
        }
        pool.upload(&mut self.uploader, &queue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pages walk the archive in id order, each row once, and end with None.
    #[test]
    fn email_local_page_walks_the_archive_in_id_order() {
        let archive = crate::archive::Archive::open_in_memory().unwrap();
        let store = archive.store();
        for i in 0..5 {
            let blob = store.blobs().intern(format!("m{i}").as_bytes()).unwrap();
            archive
                .connection()
                .execute(
                    "INSERT INTO emails (blob_id,received_at,mailbox_ids,keywords,message_match)
                     VALUES (?1,'2020-01-01T00:00:00Z','[]','[]','{}')",
                    rusqlite::params![blob],
                )
                .unwrap();
        }
        let mut seen = Vec::new();
        let mut after = None;
        let mut pages = 0;
        while let Some(page) = email_local_page(store, after, 2).unwrap() {
            assert!(page.len() <= 2);
            after = page.last().map(|(id, _)| *id);
            seen.extend(page.into_iter().map(|(id, _)| id));
            pages += 1;
        }
        assert_eq!(pages, 3);
        let mut sorted = seen.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(seen, sorted);
        assert_eq!(seen.len(), 5);
    }

    fn limits(set: u64) -> Limits {
        Limits {
            max_objects_in_get: 500,
            max_objects_in_set: set,
            max_calls_in_request: 16,
            max_concurrent_requests: 4,
            max_concurrent_upload: 4,
            max_size_request: 10_000_000,
            max_size_upload: 50_000_000,
        }
    }

    #[test]
    fn batch_size_follows_max_objects_in_set() {
        assert_eq!(EmailBatch::initial_size(&limits(500)), 500);
        assert_eq!(EmailBatch::initial_size(&limits(7)), 7);
        assert_eq!(EmailBatch::initial_size(&limits(0)), 1);
    }

    #[test]
    fn reupload_batches_halve_down_to_one() {
        let mut size = 500;
        let mut steps = Vec::new();
        while size > 1 {
            size = EmailBatch::halve(size);
            steps.push(size);
        }
        assert_eq!(steps, vec![250, 125, 62, 31, 15, 7, 3, 1]);
        assert_eq!(EmailBatch::halve(1), 1);
        assert_eq!(EmailBatch::halve(0), 1);
    }

    #[test]
    fn chunking_yields_ceil_n_over_limit_batches() {
        let ids: Vec<u32> = (0..1001).collect();
        let sizes: Vec<usize> = ids
            .chunks(EmailBatch::initial_size(&limits(500)))
            .map(<[u32]>::len)
            .collect();
        assert_eq!(sizes, vec![500, 500, 1]);
    }

    #[test]
    fn blob_hint_and_size_note_texts() {
        let hint = BlobHint {
            mid: Some("a@b".to_owned()),
            len: Some(2048),
        };
        assert_eq!(hint.to_string(), "message-id <a@b>, 2.0 KB");
        let bare = BlobHint {
            mid: None,
            len: None,
        };
        assert_eq!(bare.to_string(), "no message-id");
        assert!(
            SizeNote(&JmapError::RequestTooLarge)
                .to_string()
                .starts_with("; exceeds")
        );
        assert_eq!(SizeNote(&JmapError::malformed("x")).to_string(), "");
    }
}
