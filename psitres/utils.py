from sqlalchemy.orm.exc import NoResultFound
import bisect
import math


def read_or_instantiate(session, model_, *index_columns, **keyword_expressions):
    try:
        if len(index_columns) > 0:
            index = {col_name:keyword_expressions[col_name] for col_name in index_columns}
        else:
            index = keyword_expressions
        return session.query(model_).filter_by(**index).one()
    except NoResultFound:
        return model_(**keyword_expressions)

    
def partition(seq, n):
    """ 
    Yield n successive partitions from seq 
    of approximately the same size.
    """
    n = int(math.ceil(len(seq) / float(n)))
    return (seq[i:i + n] for i in xrange(0, len(seq), n))


def take_closest(seq, n):
    """
    Assumes seq is sorted. Returns closest value to n.

    If two numbers are equally close, return the smallest number.
    
    This is generally faster than: 
        min(seq, key=lambda x:abs(x - n))
    """
    pos = bisect.bisect_left(seq, n)
    if pos == 0:
        return seq[0]
    if pos == len(seq):
        return seq[-1]
    before = seq[pos - 1]
    after = seq[pos]
    if after - n < n - before:
        return after    
    else:
        return before
