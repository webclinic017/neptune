import datetime


def get_epoch_time_units(value: float) -> str:
    """Determine Epoch time units based on value and reasonable ranges
    @param value: epoch time
    @return: str representation of time unit
    """
    if value > 946731600000000000:
        return 'ns'
    elif value > 946731600000000:
        return 'us'
    elif value > 946731600000:
        return 'ms'
    elif value > 946731600:
        return 's'
    else:
        raise Exception("Invalid Epoch time, unknown units")


def round_datetime(dt: datetime.datetime, interval=datetime.timedelta(minutes=1)) -> datetime.datetime:
    """Round a datetime object to a multiple of a timedelta
    @param dt: datetime object to round
    @param interval: datetime.timedelta specifying rounding interval

    @return rounded datetime
    """
    interval_s = interval.total_seconds()
    seconds = (dt - dt.min).seconds
    rounding = (seconds + interval_s / 2) // interval_s * interval_s
    return dt + datetime.timedelta(0, rounding - seconds, -dt.microsecond)
