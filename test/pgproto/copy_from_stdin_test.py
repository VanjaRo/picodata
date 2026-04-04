import psycopg
import pytest

from conftest import Postgres
from test.pgproto.copy_test_utils import connect_admin, count_rows, create_test_table


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


def test_copy_is_atomic_on_late_error(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_atomic_late_error")

        prefix_rows = [f"{idx}\trow-{idx}\n" for idx in range(1, 257)]
        tail_rows = [f"{idx}\ttail-{idx}\n" for idx in range(600, 603)]
        with pytest.raises(psycopg.Error, match=r"(failed to bind parameter|not a valid int|invalid input syntax)"):
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_atomic_late_error" ("id", "value") FROM STDIN') as copy:
                    for row in prefix_rows:
                        copy.write(row)
                    copy.write("bad\tboom\n")
                    for row in tail_rows:
                        copy.write(row)

        assert count_rows(conn, "copy_atomic_late_error") == 0


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


def test_copy_rejects_multi_node(postgres: Postgres):
    postgres.cluster.add_instance(wait_online=True, replicaset_name="copy_test_rs2")

    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_multi_node")

        with pytest.raises(psycopg.Error, match="single-node clusters"):
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_multi_node" ("id", "value") FROM STDIN') as copy:
                    copy.write("1\ta\n")


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


def test_copy_rejects_trailing_escape_and_connection_recovers(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_trailing_escape")

        with pytest.raises(psycopg.Error, match="COPY data ended inside an escape sequence"):
            with conn.cursor() as cur:
                with cur.copy('COPY "copy_trailing_escape" ("id", "value") FROM STDIN') as copy:
                    copy.write("1\tbroken\\")

        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_trailing_escape") == 0
