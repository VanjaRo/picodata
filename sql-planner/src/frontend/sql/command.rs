use super::ast::{ParseTree, Rule};
use crate::errors::{Entity, SbroadError};
use crate::{
    CopyFormat, CopyFrom, CopyInput, CopyOptions, CopyOutput, CopyStatement, CopyTableTarget,
    CopyTo, CopyToSource,
};
use pest::iterators::Pair;
use pest::Parser;
use smol_str::format_smolstr;

#[derive(Debug)]
pub(crate) enum ParsedCommand {
    Sql,
    Copy(CopyStatement),
}

pub(crate) fn parse_command(query: &str) -> Result<ParsedCommand, SbroadError> {
    let top_pair = parse_top_level_command(query)?;

    match top_pair.as_rule() {
        Rule::Copy => parse_copy(top_pair).map(ParsedCommand::Copy),
        _ => Ok(ParsedCommand::Sql),
    }
}

fn parse_top_level_command<'query>(query: &'query str) -> Result<Pair<'query, Rule>, SbroadError> {
    let mut command_pair = ParseTree::parse(Rule::Command, query)
        .map_err(|e| SbroadError::ParsingError(Entity::Rule, format_smolstr!("{e}")))?;
    Ok(command_pair
        .next()
        .expect("Query expected as a first parsing tree child."))
}

fn parse_copy(pair: Pair<'_, Rule>) -> Result<CopyStatement, SbroadError> {
    debug_assert_eq!(pair.as_rule(), Rule::Copy);

    let mut table_name = None;
    let mut columns = Vec::new();
    let mut direction = None;
    let mut endpoint = None;
    let mut options = CopyOptions::default();

    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::CopyTableName => table_name = Some(child.as_str().to_string()),
            Rule::Identifier => columns.push(child.as_str().to_string()),
            Rule::CopyDirection => {
                direction = Some(match child.as_str().to_ascii_lowercase().as_str() {
                    "from" => ParsedDirection::From,
                    "to" => ParsedDirection::To,
                    unexpected => {
                        return Err(SbroadError::ParsingError(
                            Entity::Query,
                            format_smolstr!("unsupported COPY direction: {unexpected}"),
                        ));
                    }
                });
            }
            Rule::CopyTarget => {
                endpoint = Some(match child.as_str().to_ascii_lowercase().as_str() {
                    "stdin" => ParsedEndpoint::Stdin,
                    "stdout" => ParsedEndpoint::Stdout,
                    unexpected => {
                        return Err(SbroadError::ParsingError(
                            Entity::Query,
                            format_smolstr!("unsupported COPY target: {unexpected}"),
                        ));
                    }
                });
            }
            Rule::CopyWithOptions => parse_with_options(&mut options, child)?,
            Rule::CopyLegacyDelimiter => options.delimiter = Some(parse_option_value(child)),
            Rule::CopyLegacyNull => options.null_string = Some(parse_option_value(child)),
            _ => {}
        }
    }

    let table = CopyTableTarget {
        table_name: table_name.ok_or_else(|| {
            SbroadError::Invalid(Entity::Query, Some("COPY table name is missing".into()))
        })?,
        columns,
    };

    let direction = direction.ok_or_else(|| {
        SbroadError::Invalid(Entity::Query, Some("COPY direction is missing".into()))
    })?;
    let endpoint = endpoint.ok_or_else(|| {
        SbroadError::Invalid(Entity::Query, Some("COPY target is missing".into()))
    })?;

    match (direction, endpoint) {
        (ParsedDirection::From, ParsedEndpoint::Stdin) => Ok(CopyStatement::From(CopyFrom {
            table,
            input: CopyInput::Stdin,
            options,
        })),
        (ParsedDirection::To, ParsedEndpoint::Stdout) => Ok(CopyStatement::To(CopyTo {
            source: CopyToSource::Table(table),
            output: CopyOutput::Stdout,
            options,
        })),
        (ParsedDirection::From, ParsedEndpoint::Stdout) => Err(SbroadError::Invalid(
            Entity::Query,
            Some("COPY FROM STDOUT is invalid".into()),
        )),
        (ParsedDirection::To, ParsedEndpoint::Stdin) => Err(SbroadError::Invalid(
            Entity::Query,
            Some("COPY TO STDIN is invalid".into()),
        )),
    }
}

fn parse_with_options(options: &mut CopyOptions, pair: Pair<'_, Rule>) -> Result<(), SbroadError> {
    for option in pair.into_inner() {
        match option.as_rule() {
            Rule::CopyFormatOption => {
                options.format = parse_copy_format(&parse_option_value(option))?;
            }
            Rule::CopyDelimiterOption => options.delimiter = Some(parse_option_value(option)),
            Rule::CopyNullOption => options.null_string = Some(parse_option_value(option)),
            Rule::CopyHeaderOption => {
                options.header = option
                    .into_inner()
                    .next()
                    .map(|value| value.as_str().eq_ignore_ascii_case("true"))
                    .unwrap_or(true);
            }
            _ => {}
        }
    }

    Ok(())
}

fn parse_copy_format(raw: &str) -> Result<CopyFormat, SbroadError> {
    match raw.to_ascii_lowercase().as_str() {
        "text" => Ok(CopyFormat::Text),
        "csv" => Ok(CopyFormat::Csv),
        "binary" => Ok(CopyFormat::Binary),
        unsupported => Err(SbroadError::Invalid(
            Entity::Query,
            Some(format_smolstr!("unsupported COPY format: {unsupported}")),
        )),
    }
}

fn parse_option_value(option: Pair<'_, Rule>) -> String {
    let value = option
        .into_inner()
        .next()
        .expect("COPY option must have a value");

    match value.as_rule() {
        Rule::Identifier => value.as_str().to_string(),
        Rule::SingleQuotedString => unquote_single_quoted(value.as_str()),
        _ => unreachable!("unexpected COPY option value rule"),
    }
}

fn unquote_single_quoted(raw: &str) -> String {
    raw[1..raw.len() - 1].replace("''", "'")
}

#[derive(Clone, Copy)]
enum ParsedDirection {
    From,
    To,
}

#[derive(Clone, Copy)]
enum ParsedEndpoint {
    Stdin,
    Stdout,
}

#[cfg(test)]
mod tests {
    use super::{parse_command, ParsedCommand};
    use crate::{CopyFormat, CopyFrom, CopyInput, CopyOptions, CopyStatement, CopyTableTarget};

    fn parse_copy(query: &str) -> CopyStatement {
        match parse_command(query).expect("parse command") {
            ParsedCommand::Copy(copy) => copy,
            ParsedCommand::Sql => panic!("expected COPY command"),
        }
    }

    #[test]
    fn parses_copy_from_stdin() {
        let parsed = parse_copy(
            r#"COPY "t" ("id", "value") FROM STDIN WITH (DELIMITER '|', NULL 'nil', HEADER true)"#,
        );

        assert_eq!(
            parsed,
            CopyStatement::From(CopyFrom {
                table: CopyTableTarget {
                    table_name: r#""t""#.into(),
                    columns: vec![r#""id""#.into(), r#""value""#.into()],
                },
                input: CopyInput::Stdin,
                options: CopyOptions {
                    format: CopyFormat::Text,
                    delimiter: Some("|".into()),
                    null_string: Some("nil".into()),
                    header: true,
                },
            })
        );
    }

    #[test]
    fn classifies_non_copy_as_sql() {
        assert!(matches!(
            parse_command("SELECT 1").unwrap(),
            ParsedCommand::Sql
        ));
    }

}
