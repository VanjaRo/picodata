import psycopg
import pytest

from conftest import Postgres, TarantoolError
from test.pgproto.copy_test_utils import (
    connect_admin,
    count_rows,
    create_test_table,
    set_admin_password,
)


def test_copy_text_basic(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_basic")

        with conn.cursor() as cur:
            with cur.copy('COPY "copy_text_basic" ("id", "value") FROM STDIN') as copy:
                copy.write("1\tal")
                copy.write("pha\n2\tbe")
                copy.write("ta\n")
            assert cur.statusmessage == "COPY 2"

        rows = conn.execute('SELECT "id", "value" FROM "copy_text_basic" ORDER BY "id"').fetchall()
        assert rows == [(1, "alpha"), (2, "beta")]


def test_copy_text_with_null_marker(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_null")

        with conn.cursor() as cur:
            with cur.copy('COPY "copy_text_null" ("id", "value") FROM STDIN') as copy:
                copy.write("1\t\\N\n2\tvalue\n")
            assert cur.statusmessage == "COPY 2"

        rows = conn.execute('SELECT "id", "value" FROM "copy_text_null" ORDER BY "id"').fetchall()
        assert rows == [(1, None), (2, "value")]


def test_copy_text_without_explicit_column_list(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_default_columns")

        with conn.cursor() as cur:
            with cur.copy('COPY "copy_text_default_columns" FROM STDIN') as copy:
                copy.write("1\talpha\n")
                copy.write("2\tbeta\n")
            assert cur.statusmessage == "COPY 2"

        rows = conn.execute(
            'SELECT "id", "value" FROM "copy_text_default_columns" ORDER BY "id"'
        ).fetchall()
        assert rows == [(1, "alpha"), (2, "beta")]


def test_copy_with_explicit_delimiter_option(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_delim")

        with conn.cursor() as cur:
            with cur.copy('COPY "copy_text_delim" ("id", "value") FROM STDIN WITH (DELIMITER \'|\')') as copy:
                copy.write("1|one\n2|two\n")
            assert cur.statusmessage == "COPY 2"

        rows = conn.execute('SELECT "id", "value" FROM "copy_text_delim" ORDER BY "id"').fetchall()
        assert rows == [(1, "one"), (2, "two")]


def test_copy_text_decodes_supported_escapes(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_escapes")

        with conn.cursor() as cur:
            with cur.copy('COPY "copy_text_escapes" ("id", "value") FROM STDIN') as copy:
                copy.write("1\thello\\\\world\n")
                copy.write("2\tline\\nfeed\n")
                copy.write("3\ttab\\tsep\n")
                copy.write("4\tascii\\141\\x42\n")
                copy.write("5\tvert\\vform\\fback\\b\n")
            assert cur.statusmessage == "COPY 5"

        rows = conn.execute('SELECT "id", "value" FROM "copy_text_escapes" ORDER BY "id"').fetchall()
        assert rows == [
            (1, "hello\\world"),
            (2, "line\nfeed"),
            (3, "tab\tsep"),
            (4, "asciiaB"),
            (5, "vert\x0bform\x0cback\x08"),
        ]


def test_copy_with_header_option(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_header")

        with conn.cursor() as cur:
            with cur.copy('COPY "copy_text_header" ("id", "value") FROM STDIN WITH (HEADER TRUE)') as copy:
                copy.write("id\tvalue\n")
                copy.write("1\tx\n")
            assert cur.statusmessage == "COPY 1"

        rows = conn.execute('SELECT "id", "value" FROM "copy_text_header" ORDER BY "id"').fetchall()
        assert rows == [(1, "x")]


def test_copy_streaming_default_commits_flushed_prefix_on_late_error(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_streaming_late_error")

        prefix_rows = [f"{idx}\trow-{idx}\n" for idx in range(1, 2049)]
        with pytest.raises(psycopg.Error, match=r"(not a valid int|invalid input syntax)"):
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_streaming_late_error" ("id", "value") FROM STDIN') as copy:
                    for row in prefix_rows:
                        copy.write(row)
                    copy.write("bad\tboom\n")

        persisted = count_rows(conn, "copy_streaming_late_error")
        assert persisted > 0
        assert persisted <= len(prefix_rows)
        first_row = conn.execute(
            'SELECT "id", "value" FROM "copy_streaming_late_error" WHERE "id" = 1'
        ).fetchone()
        assert first_row == (1, "row-1")


def test_copy_batch_size_controls_streaming_flush_granularity(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_batch_size_late_error")

        with pytest.raises(psycopg.Error, match=r"(not a valid int|invalid input syntax)"):
            with conn.cursor() as cur:
                with cur.copy(
                    'COPY "copy_batch_size_late_error" ("id", "value") FROM STDIN WITH (BATCH_SIZE = 2)'
                ) as copy:
                    copy.write("1\tone\n")
                    copy.write("2\ttwo\n")
                    copy.write("3\tthree\n")
                    copy.write("4\tfour\n")
                    copy.write("bad\tboom\n")

        rows = conn.execute(
            'SELECT "id", "value" FROM "copy_batch_size_late_error" ORDER BY "id"'
        ).fetchall()
        assert rows == [(1, "one"), (2, "two"), (3, "three"), (4, "four")]


def test_copy_batch_size_persists_flushed_prefix_on_late_bad_copy_file_format(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_batch_size_late_format_error")

        with pytest.raises(psycopg.Error, match="COPY row has 1 columns but expected 2") as exc_info:
            with conn.cursor() as cur:
                with cur.copy(
                    'COPY "copy_batch_size_late_format_error" ("id", "value") FROM STDIN WITH (BATCH_SIZE = 2)'
                ) as copy:
                    copy.write("1\tone\n")
                    copy.write("2\ttwo\n")
                    copy.write("3\tthree\n")
                    copy.write("4\tfour\n")
                    copy.write("5\n")

        assert exc_info.value.sqlstate == "22P04"
        rows = conn.execute(
            'SELECT "id", "value" FROM "copy_batch_size_late_format_error" ORDER BY "id"'
        ).fetchall()
        assert rows == [(1, "one"), (2, "two"), (3, "three"), (4, "four")]
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_client_fail_aborts_and_connection_recovers(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_fail_recovery")

        with pytest.raises(psycopg.Error, match=r"client aborted copy"):
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_fail_recovery" ("id", "value") FROM STDIN') as copy:
                    copy.write("1\twill_be_aborted\n")
                    raise RuntimeError("client aborted copy")

        assert count_rows(conn, "copy_fail_recovery") == 0
        ping = conn.execute("SELECT 1").fetchone()
        assert ping == (1,)


def test_copy_batch_size_preserves_flushed_prefix_on_client_abort(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_fail_after_flush")

        with pytest.raises(psycopg.Error, match=r"client aborted copy"):
            with conn.cursor() as cur:
                with cur.copy(
                    'COPY "copy_fail_after_flush" ("id", "value") FROM STDIN WITH (BATCH_SIZE = 2)'
                ) as copy:
                    copy.write("1\tone\n")
                    copy.write("2\ttwo\n")
                    copy.write("3\tthree\n")
                    copy.write("4\tfour\n")
                    raise RuntimeError("client aborted copy")

        rows = conn.execute(
            'SELECT "id", "value" FROM "copy_fail_after_flush" ORDER BY "id"'
        ).fetchall()
        assert rows == [(1, "one"), (2, "two"), (3, "three"), (4, "four")]
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_works_on_multi_node_cluster(postgres: Postgres):
    postgres.cluster.add_instance(wait_online=True, replicaset_name="copy_test_rs2")

    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_multi_node")

        with conn.cursor() as cur:
            with cur.copy('COPY "copy_multi_node" ("id", "value") FROM STDIN') as copy:
                copy.write("1\tone\n2\ttwo\n3\tthree\n")
            assert cur.statusmessage == "COPY 3"

        rows = conn.execute(
            'SELECT "id", "value" FROM "copy_multi_node" ORDER BY "id"'
        ).fetchall()
        assert rows == [(1, "one"), (2, "two"), (3, "three")]

def test_copy_text_with_crlf_and_final_line_without_newline(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_crlf")

        with conn.cursor() as cur:
            with cur.copy('COPY "copy_crlf" ("id", "value") FROM STDIN') as copy:
                copy.write("1\talpha\r\n2\tbeta")
            assert cur.statusmessage == "COPY 2"

        rows = conn.execute('SELECT "id", "value" FROM "copy_crlf" ORDER BY "id"').fetchall()
        assert rows == [(1, "alpha"), (2, "beta")]


def test_copy_text_escaped_physical_newline(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_escaped_newline")

        with conn.cursor() as cur:
            with cur.copy('COPY "copy_escaped_newline" ("id", "value") FROM STDIN') as copy:
                copy.write("1\thello\\\nworld\n")
            assert cur.statusmessage == "COPY 1"

        rows = conn.execute('SELECT "id", "value" FROM "copy_escaped_newline" ORDER BY "id"').fetchall()
        assert rows == [(1, "hello\nworld")]


def test_copy_text_treats_backslash_dot_as_data(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_backslash_dot")

        with conn.cursor() as cur:
            with cur.copy('COPY "copy_backslash_dot" ("id", "value") FROM STDIN') as copy:
                copy.write("1\t\\.\n")
            assert cur.statusmessage == "COPY 1"

        rows = conn.execute('SELECT "id", "value" FROM "copy_backslash_dot" ORDER BY "id"').fetchall()
        assert rows == [(1, ".")]


def test_copy_text_rejects_mixed_line_endings(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_mixed_line_endings")

        with pytest.raises(psycopg.Error, match="mixed line endings"):
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_mixed_line_endings" ("id", "value") FROM STDIN') as copy:
                    copy.write("1\talpha\n2\tbeta\r\n")

        assert count_rows(conn, "copy_mixed_line_endings") == 0


def test_copy_empty_string_is_not_null(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_empty_string")

        with conn.cursor() as cur:
            with cur.copy('COPY "copy_empty_string" ("id", "value") FROM STDIN') as copy:
                copy.write("1\t\n")
                copy.write("2\t\\N\n")
            assert cur.statusmessage == "COPY 2"

        rows = conn.execute('SELECT "id", "value" FROM "copy_empty_string" ORDER BY "id"').fetchall()
        assert rows == [(1, ""), (2, None)]


def test_copy_rejects_too_few_columns_without_partial_rows(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_too_few")

        # psycopg still sends CopyDone while unwinding a failed COPY context manager,
        # so same-connection recovery is asserted in the raw protocol suite instead.
        with pytest.raises(psycopg.Error, match="COPY row has 1 columns but expected 2"):
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_too_few" ("id", "value") FROM STDIN') as copy:
                    copy.write("1\n")

    with connect_admin(postgres) as check_conn:
        assert check_conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(check_conn, "copy_too_few") == 0


def test_copy_rejects_too_many_columns_without_partial_rows(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_too_many")

        # psycopg still sends CopyDone while unwinding a failed COPY context manager,
        # so same-connection recovery is asserted in the raw protocol suite instead.
        with pytest.raises(psycopg.Error, match="COPY row has 3 columns but expected 2"):
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_too_many" ("id", "value") FROM STDIN') as copy:
                    copy.write("1\talpha\textra\n")

    with connect_admin(postgres) as check_conn:
        assert check_conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(check_conn, "copy_too_many") == 0


def test_copy_rejects_duplicate_columns_in_column_list(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_duplicate_columns")

        with pytest.raises(psycopg.Error, match='column "id" specified more than once') as exc_info:
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_duplicate_columns" ("id", "id") FROM STDIN'):
                    pass

        assert exc_info.value.sqlstate == "42701"
        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_duplicate_columns") == 0


def test_copy_rejects_unknown_column_in_column_list(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_unknown_column")

        with pytest.raises(psycopg.Error, match='column does not exist: missing') as exc_info:
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_unknown_column" ("id", "missing") FROM STDIN'):
                    pass

        assert exc_info.value.sqlstate == "42703"
        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_unknown_column") == 0


def test_copy_rejects_system_bucket_id_column_in_column_list(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_system_bucket_id")

        with pytest.raises(
            psycopg.Error, match='system column "bucket_id" cannot be inserted'
        ) as exc_info:
            with conn.cursor() as cur:
                with cur.copy(
                    'COPY "copy_system_bucket_id" ("bucket_id", "id", "value") FROM STDIN'
                ):
                    pass

        assert exc_info.value.sqlstate == "42P10"
        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_system_bucket_id") == 0


def test_copy_rejects_missing_required_columns_in_column_list(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_missing_required_column")

        with pytest.raises(psycopg.Error, match='NonNull column "id" must be specified') as exc_info:
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_missing_required_column" ("value") FROM STDIN'):
                    pass

        assert exc_info.value.sqlstate == "23502"
        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_missing_required_column") == 0


def test_copy_rejects_missing_required_global_bucket_id_column(postgres: Postgres):
    with connect_admin(postgres) as conn:
        conn.execute(
            """
            CREATE TABLE "copy_missing_global_bucket_id" (
                "id" INT PRIMARY KEY,
                "bucket_id" INT NOT NULL,
                "value" TEXT
            ) DISTRIBUTED GLOBALLY
            """
        )

        with pytest.raises(
            psycopg.Error, match='NonNull column "bucket_id" must be specified'
        ) as exc_info:
            with conn.cursor() as cur:
                with cur.copy(
                    'COPY "copy_missing_global_bucket_id" ("id", "value") FROM STDIN'
                ):
                    pass

        assert exc_info.value.sqlstate == "23502"
        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_missing_global_bucket_id") == 0


def test_copy_requires_write_privilege(postgres: Postgres):
    user = "copy_no_write"
    password = "P@ssw0rd"

    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_acl_denied")

    postgres.instance.sql(f'CREATE USER "{user}" WITH PASSWORD \'{password}\' USING md5')

    with psycopg.connect(
        f"user={user} password={password} host={postgres.host} port={postgres.port} sslmode=disable"
    ) as conn:
        conn.autocommit = True

        with pytest.raises(psycopg.Error) as exc_info:
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_acl_denied" ("id", "value") FROM STDIN'):
                    pass

        assert exc_info.value.sqlstate == "42501"
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_rejects_unsupported_copy_format(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_unsupported_format")

        with pytest.raises(
            psycopg.Error, match="COPY format csv is not supported"
        ) as exc_info:
            with conn.cursor() as cur:
                with cur.copy(
                    'COPY "copy_unsupported_format" ("id", "value") FROM STDIN WITH (FORMAT CSV)'
                ):
                    pass

        assert exc_info.value.sqlstate == "0A000"
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_rejects_non_operable_table_at_start(postgres: Postgres):
    password = "P@ssw0rd"
    set_admin_password(postgres, password)

    error_injection = "BLOCK_GOVERNOR_BEFORE_DDL_COMMIT"
    postgres.instance.call("pico._inject_error", error_injection, True)
    try:
        with pytest.raises((TimeoutError, TarantoolError), match="timeout"):
            postgres.instance.sql(
                """
                CREATE TABLE "copy_not_operable" (
                    "id" integer not null,
                    "value" string,
                    primary key ("id")
                )
                using memtx distributed by ("id")
                option (timeout = 1)
                """
            )

        with psycopg.connect(
            f"user=admin password={password} host={postgres.host} port={postgres.port} sslmode=disable"
        ) as conn:
            conn.autocommit = True

            with pytest.raises(
                psycopg.Error, match="cannot be modified now as DDL operation is in progress"
            ) as exc_info:
                with conn.cursor() as cur:
                    with cur.copy('COPY "copy_not_operable" ("id", "value") FROM STDIN'):
                        pass

            assert exc_info.value.sqlstate == "55000"
            assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        postgres.instance.call("pico._inject_error", error_injection, False)


def test_copy_rejects_trailing_escape_and_connection_recovers(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_trailing_escape")

        with pytest.raises(psycopg.Error, match="COPY data ended inside an escape sequence"):
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_trailing_escape" ("id", "value") FROM STDIN') as copy:
                    copy.write("1\tbroken\\")

        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_trailing_escape") == 0
