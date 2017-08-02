
import datetime as dt


class date_math:

    def __init__(self,date = None):
        if date is None:
            self._date = dt.datetime.today()
        else:
            self._date = date

    def month_add(self,months):
        thisyear = self._date.year
        thismonth = self._date.month

        monthdiff = months % 12
        yeardiff = (months - monthdiff)/12
        month_added = thismonth + monthdiff
        if month_added>12:
            yeardiff +=1
            month_final = month_added - 12
        elif month_added<0:
            yeardiff -=1
            month_final = month_added + 12
        else:
            month_final = month_added
        year_final = int(thisyear + yeardiff)
        assert year_final>=0
        if month_final==2 and self._date.day>28:
            date_final = 28
        elif month_final in (4,6,9,11) and self._date.day==31:
            date_final = 30
        else:
            date_final = self._date.day
        return dt.datetime(year=year_final,month=month_final,day=date_final)
