"""Progress must distinguish persisted tables from lazy DataFrames."""
from unittest.mock import MagicMock

import pytest

from databay.etl import Octopus


@pytest.mark.parametrize("table_format", ["delta", "iceberg"])
@pytest.mark.parametrize("fail_second", [False, True])
def test_feed_reports_current_table(capsys, table_format, fail_second):
    spark = MagicMock()
    frame = spark.read.format.return_value.option.return_value.option.return_value.option.return_value.options.return_value.load.return_value
    writer = (frame.writeTo.return_value.using.return_value.createOrReplace
              if table_format == "iceberg" else
              frame.write.format.return_value.mode.return_value.saveAsTable)
    writer.side_effect = [None, RuntimeError("write failed")] if fail_second else None
    octopus = Octopus(spark=spark, engine="postgresql",
                      connection_provider=lambda: {"HOST": "localhost", "DATABASE": "db", "USER": "user", "PASSWORD": "secret"})
    queries = [("SELECT 1", "first"), ("SELECT 2", "second")]
    if fail_second:
        with pytest.raises(RuntimeError, match="write failed"):
            octopus.feed_spark(queries, "raw", table_format=table_format)
    else:
        octopus.feed_spark(queries, "raw", table_format=table_format)
    output = capsys.readouterr().out
    assert output == "\r[1/2] first\r[2/2] second"
    assert "secret" not in output


def test_read_reports_lazy_frames_without_triggering_actions(capsys):
    spark = MagicMock()
    octopus = Octopus(spark=spark, engine="postgresql",
                      connection_provider=lambda: {"HOST": "localhost", "DATABASE": "db", "USER": "user", "PASSWORD": "secret"})
    frames = octopus.read_jdbc(iter([("SELECT 1", "first"), ("SELECT 2", "second")]))
    assert capsys.readouterr().out == "\r[1/2] first\r[2/2] second"
    for frame in frames.values():
        frame.collect.assert_not_called()
        frame.count.assert_not_called()



def test_notebook_progress_updates_one_display(monkeypatch):
    from databay.etl.octopus import _table_progress
    import IPython
    import IPython.display

    handle = MagicMock()
    display = MagicMock(return_value=handle)
    monkeypatch.setattr(IPython, "get_ipython", lambda: MagicMock(kernel=True))
    monkeypatch.setattr(IPython.display, "display", display)
    update = _table_progress(2)
    update(1, "customers")
    update(2, "orders")
    display.assert_called_once_with("[1/2] customers", display_id=True)
    handle.update.assert_called_once_with("[2/2] orders")



@pytest.mark.parametrize("single", [True, False])
def test_write_reports_current_table(capsys, single):
    frame = MagicMock()
    octopus = Octopus(
        spark=MagicMock(), engine="postgresql",
        connection_provider=lambda: {
            "HOST": "localhost", "DATABASE": "db",
            "USER": "user", "PASSWORD": "secret",
        },
    )
    if single:
        octopus.write_jdbc(frame, target_table="first", target_schema="public")
        expected = "\r[1/1] first"
    else:
        octopus.write_jdbc(
            {"first": frame, "second": frame}, target_schema="public",
        )
        expected = "\r[1/2] first\r[2/2] second"
    assert capsys.readouterr().out == expected



@pytest.mark.parametrize("notebook", [False, True])
def test_progress_escapes_control_characters(monkeypatch, capsys, notebook):
    import IPython
    import IPython.display
    from databay.etl.octopus import _table_progress

    display = MagicMock()
    monkeypatch.setattr(IPython, "get_ipython",
                        lambda: MagicMock(kernel=True) if notebook else None)
    monkeypatch.setattr(IPython.display, "display", display)
    name = "orders" + chr(27) + "[2J" + chr(10) + chr(13) + chr(0x202e)
    _table_progress(1)(1, name)
    expected = r"[1/1] orders\x1b[2J\n\r\u202e"
    if notebook:
        display.assert_called_once_with(expected, display_id=True)
    else:
        assert capsys.readouterr().out == "\r" + expected


def test_broken_display_does_not_abort_write(monkeypatch):
    import IPython
    import IPython.display

    monkeypatch.setattr(IPython, "get_ipython", lambda: MagicMock(kernel=True))
    display = MagicMock(side_effect=OSError("private output error"))
    monkeypatch.setattr(IPython.display, "display", display)
    frame = MagicMock()
    octopus = Octopus(
        spark=MagicMock(), engine="postgresql",
        connection_provider=lambda: {
            "HOST": "localhost", "DATABASE": "db", "USER": "user", "PASSWORD": "secret",
        },
    )
    octopus.write_jdbc({"first": frame, "second": frame})
    writer = frame.write.format.return_value.option.return_value.option.return_value
    writer = writer.option.return_value.option.return_value.options.return_value.mode.return_value
    assert writer.save.call_count == 2
    display.assert_called_once()
