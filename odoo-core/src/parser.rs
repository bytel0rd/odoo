use std::time::Duration;

use log::error;

use crate::encoder::Message;
use crate::helpers::hash_key_to_unsigned_int;

/**
Commands

SET {key} {value} {duration?}
GET {key}
DELETE {key}

APPEND {stream} {value} {duration?}
RESUME {stream} {last_time?} {limit?} {replay?}

 */
#[derive(Debug, Clone)]
pub struct CommandUpdate {
    pub value: Vec<u8>,
    pub duration: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct StreamUpdate {
    pub checkpoint: Option<u64>,
    pub limit: Option<u64>,
    pub replay: Option<bool>,
}


#[derive(Debug, Clone)]
pub enum Command {
    SET(String, CommandUpdate),
    GET(String),
    DELETE(String),
    APPEND(String, CommandUpdate),
    RESUME(String, StreamUpdate),
}

impl Command {
    pub fn get_event_key(&self) -> u64 {
        return match self {
            Command::SET(key, _) => hash_key_to_unsigned_int(key.as_bytes()),
            Command::GET(key) => hash_key_to_unsigned_int(key.as_bytes()),
            Command::DELETE(key) => hash_key_to_unsigned_int(key.as_bytes()),
            Command::APPEND(key, _) => hash_key_to_unsigned_int(key.as_bytes()),
            Command::RESUME(key, _) => hash_key_to_unsigned_int(key.as_bytes())
        };
    }
}


impl Command {
    pub fn parse_command(message: &Message) -> Result<Self, ParserError> {
        let cmds = &message.data;
        if let Some(cmd) = cmds.first() {
            let cmd = String::from_utf8(cmd.to_vec())
                .map_err(|e| {
                    error!("Unable to parse command {}", &e);
                    ParserError::InvalidCommand
                })?;

            let subcommands = cmds.split_at(1).1.iter().map(|v| v.to_owned()).collect::<Vec<_>>();
            return match cmd.as_str() {
                "GET" => Command::parse_get_command(subcommands),
                "SET" => Command::parse_set_command(subcommands),
                "DELETE" => Command::parse_delete_command(subcommands),
                "APPEND" => Command::parse_append_command(subcommands),
                "RESUME" => Command::parse_resume_command(subcommands),
                _ => Err(ParserError::InvalidCommand)
            };
        }

        return Err(ParserError::InvalidCommand);
    }

    // GET {key}
    fn parse_get_command(subcommands: Vec<Vec<u8>>) -> Result<Self, ParserError> {
        if subcommands.len() != 1 {
            return Err(ParserError::InvalidCommandArguments("GET".to_string()));
        }

        let mut set_key = "".to_string();
        if let Some(key) = subcommands.get(0) {
            set_key = String::from_utf8(key.to_vec())
                .map_err(|e| {
                    error!("Unable to parse key {}", &e);
                    ParserError::InvalidCommandArguments("key".to_string())
                })?;
        }

        Ok(Self::GET(set_key))
    }

    // DELETE {key}
    fn parse_delete_command(subcommands: Vec<Vec<u8>>) -> Result<Self, ParserError> {
        if subcommands.len() != 1 {
            return Err(ParserError::InvalidCommandArguments("DELETE".to_string()));
        }

        let mut set_key = "".to_string();
        if let Some(key) = subcommands.get(0) {
            set_key = String::from_utf8(key.to_vec())
                .map_err(|e| {
                    error!("Unable to parse key {}", &e);
                    ParserError::InvalidCommandArguments("key".to_string())
                })?;
        }

        Ok(Self::DELETE(set_key))
    }


    // SET {key} {value} {duration?}
    fn parse_set_command(subcommands: Vec<Vec<u8>>) -> Result<Self, ParserError> {
        if subcommands.len() < 2 || subcommands.len() > 3 {
            return Err(ParserError::InvalidCommandArguments("SET".to_string()));
        }

        let mut command_params = CommandUpdate {
            value: vec![],
            duration: None,
        };

        let mut set_key = "".to_string();
        if let Some(key) = subcommands.get(0) {
            set_key = String::from_utf8(key.to_vec())
                .map_err(|e| {
                    error!("Unable to parse key {}", &e);
                    ParserError::InvalidCommandArguments("key".to_string())
                })?;
        }

        if let Some(value) = subcommands.get(1) {
            command_params.value = value.to_owned();
        }

        if let Some(duration) = subcommands.get(2) {
            let duration = String::from_utf8(duration.to_vec())
                .map_err(|e| {
                    error!("Unable to parse duration {}", &e);
                    ParserError::InvalidDurationArgument
                })?;
            let has_no_duration = duration.eq_ignore_ascii_case("NULL") || duration.eq_ignore_ascii_case("-1");
            if !has_no_duration {
                let duration: i64 = duration.parse()
                    .map_err(|e| ParserError::InvalidDurationArgument)?;
                if duration > 0 {
                    command_params.duration = Some(Duration::from_millis(duration as u64));
                }
            }
        }
        Ok(Self::SET(set_key, command_params))
    }

    // APPEND {stream} {value} {duration?}
    fn parse_append_command(subcommands: Vec<Vec<u8>>) -> Result<Self, ParserError> {
        if subcommands.len() < 2 || subcommands.len() > 3 {
            return Err(ParserError::InvalidCommandArguments("APPEND".to_string()));
        }

        let mut command_params = CommandUpdate {
            value: vec![],
            duration: None,
        };

        let mut set_key = "".to_string();
        if let Some(key) = subcommands.get(0) {
            set_key = String::from_utf8(key.to_vec())
                .map_err(|e| {
                    error!("Unable to parse key {}", &e);
                    ParserError::InvalidCommandArguments("key".to_string())
                })?;
        }

        if let Some(value) = subcommands.get(1) {
            command_params.value = value.to_vec();
        }

        if let Some(duration) = subcommands.get(2) {
            let duration = String::from_utf8(duration.to_vec())
                .map_err(|e| {
                    error!("Unable to parse duration {}", &e);
                    ParserError::InvalidDurationArgument
                })?;
            let has_no_duration = duration.eq_ignore_ascii_case("NULL") || duration.eq_ignore_ascii_case("-1");
            if !has_no_duration {
                let duration: i64 = duration.parse()
                    .map_err(|e| ParserError::InvalidDurationArgument)?;
                if duration > 0 {
                    command_params.duration = Some(Duration::from_millis(duration as u64));
                }
            }
        }
        Ok(Self::APPEND(set_key, command_params))
    }

    // RESUME {stream} {last_time?} {limit?} {replay?}
    fn parse_resume_command(subcommands: Vec<Vec<u8>>) -> Result<Self, ParserError> {
        if subcommands.len() < 1 || subcommands.len() > 4 {
            return Err(ParserError::InvalidCommandArguments("APPEND".to_string()));
        }

        let mut command_params = StreamUpdate {
            checkpoint: None,
            limit: None,
            replay: None,
        };

        let mut set_key = "".to_string();
        if let Some(key) = subcommands.get(0) {
            set_key = String::from_utf8(key.to_vec())
                .map_err(|e| {
                    error!("Unable to key {}", &e);
                    ParserError::InvalidCommandArguments("key".to_string())
                })?;
        }

        if let Some(checkpoint) = subcommands.get(1) {
            let checkpoint = String::from_utf8(checkpoint.to_vec())
                .map_err(|e| {
                    error!("Unable to parse checkpoint {}", &e);
                    ParserError::InvalidCheckpointArgument
                })?;
            let has_no_duration = checkpoint.eq_ignore_ascii_case("NULL") || checkpoint.eq_ignore_ascii_case("-1");
            if !has_no_duration {
                let checkpoint: i64 = checkpoint.parse()
                    .map_err(|e| ParserError::InvalidCheckpointArgument)?;
                command_params.checkpoint = Some(checkpoint as u64);
            }
        }

        if let Some(limit) = subcommands.get(2) {
            let limit = String::from_utf8(limit.to_vec())
                .map_err(|e| {
                    error!("Unable to parse limit {}", &e);
                    ParserError::InvalidLimitArgument
                })?;
            let has_no_duration = limit.eq_ignore_ascii_case("NULL") || limit.eq_ignore_ascii_case("-1");
            if !has_no_duration {
                let limit: i64 = limit.parse()
                    .map_err(|e| ParserError::InvalidLimitArgument)?;
                command_params.limit = Some(limit as u64);
            }
        }

        if let Some(replay) = subcommands.get(3) {
            let replay = String::from_utf8(replay.to_vec())
                .map_err(|e| {
                    error!("Unable to parse replay {}", &e);
                    ParserError::InvalidReplayArgument
                })?;
            let has_no_replay = replay.eq_ignore_ascii_case("NULL");
            if !has_no_replay {
                let replay: bool = replay.parse()
                    .map_err(|e| ParserError::InvalidReplayArgument)?;
                command_params.replay = Some(replay);
            }
        }
        Ok(Self::RESUME(set_key, command_params))
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParserError {
    #[error("Invalid commands provided")]
    InvalidCommand,
    #[error("invalid arguments for command: {0}")]
    InvalidCommandArguments(String),
    #[error("Invalid duration value")]
    InvalidDurationArgument,
    #[error("Invalid replay value")]
    InvalidReplayArgument,
    #[error("Invalid limit value")]
    InvalidLimitArgument,
    #[error("Invalid checkpoint value")]
    InvalidCheckpointArgument,
}


