/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::path::PathBuf;

use clap::{ArgGroup, Args};

use super::Action;
use super::auth::{AccountFlags, AuthArgs};
use super::global::GlobalArgs;
use crate::error::Error;
use crate::export::{Confirm, DEFAULT_UPLOAD_BUFFER, Export, Prune};
use crate::jmap::connect::ConnectConfig;
use crate::types::ObjectTypes;

#[derive(Args)]
#[command(group(ArgGroup::new("account").required(true).args(["account_id", "account_name"])))]
pub(super) struct ExportArgs {
    #[arg(long, value_name = "URL", help = "Base URL or full JMAP session URL")]
    url: String,

    #[command(flatten)]
    auth: AuthArgs,

    #[arg(long, value_name = "ID", help = "Target account id")]
    account_id: Option<String>,

    #[arg(long, value_name = "NAME", help = "Target account name")]
    account_name: Option<String>,

    #[arg(
        long,
        value_name = "LIST",
        help = "Types to export (default: all present in archive)"
    )]
    objects: Option<String>,

    #[arg(long, help = "Delete target objects that match nothing in the archive")]
    prune: bool,

    #[arg(long, help = "Skip the interactive --prune confirmation")]
    yes: bool,

    #[arg(
        long,
        value_name = "MIB",
        default_value_t = DEFAULT_UPLOAD_BUFFER / (1024 * 1024),
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Max MiB of message data read ahead of the parallel uploads"
    )]
    upload_buffer: u64,

    #[command(flatten)]
    global: GlobalArgs,

    #[arg(value_name = "ARCHIVE", help = "Local SQLite archive")]
    archive: PathBuf,
}

impl ExportArgs {
    pub(super) fn resolve(self) -> Result<Action, Error> {
        let common = self.global.common_config(self.archive);
        let auth = self.auth.resolve()?;
        let account = AccountFlags {
            id: self.account_id,
            name: self.account_name,
        }
        .resolve()?;
        let objects = self
            .objects
            .as_deref()
            .map(str::parse::<ObjectTypes>)
            .transpose()?;

        Ok(Action::Export(
            common,
            Export {
                connect: ConnectConfig {
                    url: self.url,
                    auth,
                    account,
                },
                objects,
                prune: if self.prune {
                    Prune::Destroy
                } else {
                    Prune::Keep
                },
                confirm: if self.yes {
                    Confirm::Assume
                } else {
                    Confirm::Prompt
                },
                upload_buffer: self.upload_buffer * 1024 * 1024,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::cli::{Cli, env_lock as lock};

    fn export_upload_buffer(extra: &[&str]) -> Result<u64, clap::Error> {
        let mut argv = vec![
            "vandelay",
            "export",
            "--url",
            "https://jmap.example/",
            "--auth-bearer",
            "tok",
            "--account-id",
            "a",
        ];
        argv.extend_from_slice(extra);
        argv.push("archive.sqlite");
        let cli = Cli::try_parse_from(argv)?;
        match cli.resolve() {
            Ok(Action::Export(_, config)) => Ok(config.upload_buffer),
            _ => panic!("expected Export"),
        }
    }

    // --upload-buffer is in MiB, defaults to DEFAULT_UPLOAD_BUFFER and rejects 0.
    #[test]
    fn export_upload_buffer_resolves() {
        let _g = lock();
        assert_eq!(export_upload_buffer(&[]).unwrap(), DEFAULT_UPLOAD_BUFFER);
        assert_eq!(
            export_upload_buffer(&["--upload-buffer", "32"]).unwrap(),
            32 * 1024 * 1024
        );
        assert!(export_upload_buffer(&["--upload-buffer", "0"]).is_err());
    }
}
