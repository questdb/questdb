//
// Created by alpel on 21/06/2021.
//

#ifndef QUESTDB_TIMESTAMPS_H
#define QUESTDB_TIMESTAMPS_H

int days_in_months[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

inline bool is_leap_year(int year) {
    if (year % 4 != 0) return false;
    if (year % 400 == 0) return true;
    if (year % 100 == 0) return false;
    return true;
}

inline int days_in_month(int year, int month) {
    int days = daysInMonths[month];

    if (month == 1 && is_leap_year(year)) {
        return days + 1;
    }

    return days;
}

inline int64_t add_month_to_timestamp(const int64_t timestamp, int months) {

}

#endif //QUESTDB_TIMESTAMPS_H
