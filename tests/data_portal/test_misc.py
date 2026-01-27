"""Test for miscellaneous stuff."""


def test_state_enum() -> None:

    from data_portal_worker.load_data import StateEnum

    try:
        raise FileNotFoundError("foo")
    except Exception as error:
        assert (
            StateEnum.from_exception(error) == StateEnum.finished_not_found.value
        )
    try:
        raise ValueError("foo")
    except Exception as error:
        assert StateEnum.from_exception(error) == StateEnum.finished_failed.value
