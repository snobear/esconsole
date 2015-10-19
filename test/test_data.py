
from esconsole import esconsole
import datetime

from nose.tools import eq_, ok_

# Create a 0 day old index
def test_index_0_days_old():
    pass
    cat_line = datetime.datetime.now().strftime("green  open   %Y-%m-%dt%H:%M:%S.000z   5   0          0            0       720b           720b")
    i = esconsole.IndexInfo(cat_line)
    eq_(i.age, 0)

# Create a 5 day old index
def test_index_5_days_old():
    pass
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=5)
    cat_line = (now - delta).strftime("green  open   %Y-%m-%dt%H:%M:%S.000z   5   0          0            0       720b           720b")
    i = esconsole.IndexInfo(cat_line)
    eq_(i.age, 5)

    eq_(i.health, 'green')

def test_age_on_index_that_doesnt_match_time_bin_naming():
    pass
    cat_line = "green  open   some_random_index_name   5   0          0            0       720b           720b"
    i = esconsole.IndexInfo(cat_line)
    eq_(i.age, -1)


def test_cat_response():
    c = esconsole.CatIndicesResponse("""green  open   some_random_index_name   5   0          0            0       720b           720b
       close  some_random_index_name2
green  open   some_random_index_name3   5   0          0            0       720b           720b"""
    )

    eq_(3, len(c))

    eq_("green", c[0].health)
    eq_("close", c[1].status)
