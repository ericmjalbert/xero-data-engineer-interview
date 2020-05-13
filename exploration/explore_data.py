"""
This is a first pass at examining the data and the problems.

The point of this was to use Pandas as a quick way to start planning out
possible difficulties with doing the selected analyses. It does not represent
production-ready code.
"""

import os
import pandas as pd


def get_df_from_log_file(filename):
    # Problems with some files not doing convert_dates properly, so instead
    # manually do it
    df = pd.read_json(filename, lines=True, convert_dates=False)

    # Convert date to UTC for consistency and calculations
    df.date = pd.to_datetime(df.date, utc=True).dt.tz_convert("UTC")
    return df


def get_list_of_data_files(folder="data"):
    files = os.listdir(folder)
    return [f"{folder}/{filename}" for filename in files]


def get_churn_df(df):
    """Gets list of accounts that churn and the login that was their last"""
    logins = df[df.log_type == "LOGIN"].dropna(axis=1)
    fake_max_day = pd.Timestamp(year=2100, month=1, day=1, tz="UTC")
    logins["next_login"] = (
        logins.sort_values("date")
        .groupby("account")["date"]
        .shift(-1, fill_value=fake_max_day)
    )
    logins["is_churn"] = (logins.next_login - logins.date).dt.days >= 30

    # Remove the last 30 days since they don't have enough data for churn
    churn_cut_off_time = logins.date.max() - pd.Timedelta(days=30)
    churns = logins[logins.date <= churn_cut_off_time]

    return churns[["account", "date", "is_churn"]]


def analyze_churn(df):
    """Customer Churn Analysis

    ASSUMPTIONS:
        * A customer has to login to do any kind of activity on Hubdoc. This
            means that we can use just log_type=login to track active and
            churned customer
        * An active user is any user that is NOT churned
        * For the purpose of this exercise once a customer has been inactive
            for 30 days they will not appear in the logs again.
        * Every Calculation is done based in UTC time, but presented in EST time
        * Ignore churn for logins in the most recent month of data (Dec 2019)
    """

    print(
        """
        A customer is deemed to have stopped using Hubdoc if they have not logged
          into the system within thirty days.
        * Provide the total number of customers who have stopped using Hubdoc month
          by month. If a customer’s last login was February 10th, 2019 and we see a
          thirty day period where no additional logins took place then we would say
          that customer has stopped using Hubdoc in February. For the purpose of
          this exercise once a customer has been inactive for 30 days they will not
          appear in the logs again. Assume that you are doing this reporting as of
          January 1st 2020.
        """
    )
    churns = get_churn_df(df)
    # Convert to EST for report
    churns["date_est"] = churns.date.dt.tz_convert("est")
    monthly = churns.set_index("date_est").resample("1M").sum()
    print(monthly["is_churn"])

    print(
        """
        Customer churn rate is defined as the number of churned customers over a
          given period divided by the number of active customers at the end of a
          given period.
        * Provide monthly customer churn rate from January 1, 2019 to December 31,
          2019. Assume that you are doing this reporting as of January 1st 2020.
          * Example: January:0.03, February: 0.05 ...
        """
    )
    monthly["active_users"] = (
        churns.set_index("date_est").resample("1M").nunique()["account"]
    )
    monthly["churn_rate"] = monthly["is_churn"] / monthly["active_users"]
    print(monthly["churn_rate"])


def analyze_plans(df):
    """Plan Upgrade and Downgrade Analysis

    ASSUMPTIONS:
        * The change_date for PLAN_CHANGE is always the same as the log's date
    """

    print(
        """
        It is important to understand what plans our customers are signing up for
          initially.
        * Provide the total number of customers who selected a plan when they signed
          up and what percentage of total signups that represents. Calculate this
          for all of 2019.
          * Example: assuming we have 2 plans: X. and Y
              Plan X: 400 signups, represents 40% of total signups.
              Plan Y: 600 signups, represents 60% of total signups.
        """
    )

    signups = df[df.log_type == "SIGN_UP"].dropna(axis=1)
    signups = signups[["date", "account", "plan"]]
    total_users = signups["account"].count()
    total_signups = signups.groupby("plan").count()
    total_signups["percent"] = total_signups["account"] / total_users * 100
    print(total_signups[["account", "percent"]])

    print(
        """
        We also want to understand how customers change plans after initial sign up.
        * Provide a breakdown of how many customers changed from their initial
          signup plan to another plan and the average number of days that customers
          took to make that change.
          * Example: assuming we have 2 plans: X. and Y
              Plan Y to Plan X: 80 customers. Average time to change: 50 days.
              Plan X to Plan Y: 0 customers. Average time to change: N/A.
        """
    )
    plans = df[df.log_type == "PLAN_CHANGE"].dropna(axis=1)
    plans["change_date"] = plans.details.apply(
        lambda x: pd.to_datetime(x.get("change_date"), utc=True).tz_convert("UTC")
    )
    plans["from"] = plans.details.apply(lambda x: x.get("from"))
    plans["to"] = plans.details.apply(lambda x: x.get("to"))
    plans = plans[["account", "change_date", "from", "to"]]

    plans = pd.merge(signups, plans, how="inner", on="account")
    plans["time_to_change"] = (plans["change_date"] - plans["date"]).dt.days
    plans["change_type"] = "from-" + plans["from"] + "-to-" + plans["to"]

    print(plans.groupby("change_type").mean()["time_to_change"])


def analyze_geo(df):
    """ Geographic Location Analysis

    ASSUMPTIONS:
        * All addresses follow the format of:
            `street, city, province <whitespace> postal_code`
        * All postal_code's are canadian postal codes:
            `<3 alphanumeric chars> <whitespace> <3 alphanumeric chars>`
        * An active user is any user that is NOT churned
    """

    print(
        """
        It is important for our sales and marketing teams to know the geological
          demographic of our customer base as it provides insights on how to
          advertise and provide support for the product.
        * Provide the number of active customers in each province at the end of the
          year.
              * Example: AB:84, PE:8, ON:224, ...
        """
    )

    def province_parser(address: str) -> str:
        """This really only works because of assumptions

        Needed to handle edgecases:
            * Postal code is not seperated from province
            * not every address has same spacing between fields (Whitehose)
        """
        remove_postal_code = address.split(" ")[:-2]
        province_info = remove_postal_code[-1]
        if len(province_info) > 2:
            print(province_info)
            province_info = province_info[-2:]
        return province_info

    signups = df[df.log_type == "SIGN_UP"].dropna(axis=1)
    signups["province"] = signups.address.apply(province_parser)
    signups = signups[["account", "province"]]
    print("All signups:")
    print(signups.groupby("province").nunique()["account"])

    churns = get_churn_df(df)
    churns = churns.groupby("account").filter(lambda x: x.max()["is_churn"] is not True)
    signups = pd.merge(signups, churns, how="inner", on="account")
    print("Only active users: ")
    print(signups.groupby("province").nunique()["account"])


def main():
    df = None
    for datafile in get_list_of_data_files():
        datafile_df = get_df_from_log_file(datafile)
        print(f"{datafile}: {datafile_df.count().to_dict()}")
        df = pd.concat([df, datafile_df], ignore_index=True, sort=False)

    # Check the log_types
    log_types = df.log_type.unique().tolist()
    print("log_types: ", log_types)

    for log_type in log_types:
        df_log_type = df[df.log_type == log_type]
        df_remove_null_columns = df_log_type.dropna(axis=1)
        non_null_columns = df_remove_null_columns.columns.tolist()
        print(f"{log_type} colums: {non_null_columns}")

    # Do specific analysis
    analyze_churn(df)
    analyze_plans(df)
    analyze_geo(df)


if __name__ == "__main__":
    main()
