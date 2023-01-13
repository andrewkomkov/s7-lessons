import datetime
import dateutil.relativedelta

def input_paths(date,depth):
    range_ = []
    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    i = 0
    while i < depth:
        date_2 = date - dateutil.relativedelta.relativedelta(days=i)
        path_ = f'/user/andrew0/data/events/date={date_2.strftime("%Y-%m-%d")}/event_type=message'
        range_.append(path_)
        i = i+1
    return(range_)