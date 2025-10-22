# coding=utf-8
import telegram
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
import seaborn as sns
import io
import pandas as pd
import requests
from datetime import datetime, timedelta
import pandahouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# The Getch class, since a folder is required in the Airflow repository, which exists only in analyst_simulator and locally on my PC

class Getch:
    def __init__(self, query, db='simulator_20250820'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.df = self.getchdf

    @property
    def getchdf(self):
        try:
            return pandahouse.read_clickhouse(self.query, connection=self.connection)
        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)        

# Default parameters that are passed to all tasks
default_args = {
    'owner': 'aleksandr_antonov_hnm5755',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 10, 2),
}

# DAG run interval = 11:01 every day
schedule_interval = '01 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_aleksandr_antonov_hnm5755_bot_advanced():

    @task
    def general_text_report():
        chat_id = 427641111
        my_token = '1111111111:AAFHHxLQAz3O6B49fZnvp6v11PDYo6aTzVQ'
        bot = telegram.Bot(token=my_token)

        core_metrics_txt = Getch("""
        SELECT 
            fa.date,
            dau,
            views,
            likes,
            likes / views AS ctr,
            messages_sent
        FROM
        (
            SELECT 
                toDate(time) AS date,
                countDistinct(user_id) AS dau,
                countIf(action='view') AS views,
                countIf(action='like') AS likes
            FROM simulator_20250820.feed_actions
            WHERE toDate(time) = today() - 1
            GROUP BY date
        ) AS fa
        LEFT JOIN
        (
            SELECT  
                toDate(time) AS date,
                count() AS messages_sent
            FROM simulator_20250820.message_actions
            WHERE toDate(time) = today() - 1
            GROUP BY date
        ) AS ma
        USING date
        """).df

        row = core_metrics_txt.iloc[0]

        report_text = (
            f"*Key Metrics Report — `News Feed & Messenger` for {row['date'].strftime('%Y-%m-%d')}*\n\n"
            f"*DAU:* {row['dau']}\n"
            f"*Views:* {row['views']}\n"
            f"*Likes:* {row['likes']}\n"
            f"*CTR:* {round(row['ctr']*100, 2)}%\n"
            f"*Messages sent:* {row['messages_sent']}"
        )
        
        bot.sendMessage(chat_id=chat_id, text=report_text, parse_mode='Markdown')
        
    @task
    def feed_and_sms_report_weekly():
        chat_id = 427641111
        my_token = '1111111111:AAFHHxLQAz3O6B49fZnvp6v11PDYo6aTzVQ'
        bot = telegram.Bot(token=my_token)
                
        core_metrics_graf = Getch("""
        SELECT 
            fa.date,
            dau,
            views,
            likes,
            likes / views AS ctr,
            messages_sent
        FROM
        (
            SELECT 
                toDate(time) AS date,
                countDistinct(user_id) AS dau,
                countIf(action='view') AS views,
                countIf(action='like') AS likes
            FROM simulator_20250820.feed_actions
            WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
            GROUP BY date
        ) AS fa
        LEFT JOIN
        (
            SELECT  
                toDate(time) AS date,
                count() AS messages_sent
            FROM simulator_20250820.message_actions
            WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
            GROUP BY date
        ) AS ma
        USING date
        """).df
        df = core_metrics_graf
        
        fig, axes = plt.subplots(3, 2, figsize=(14, 10))
        axes = axes.flatten()

        # 1. DAU
        axes[0].plot(df['date'], df['dau'], marker='o')
        axes[0].set_title('DAU')

        # 2. Views
        axes[1].plot(df['date'], df['views'], marker='o', color='orange')
        axes[1].set_title('Views')

        # 3. Likes
        axes[2].plot(df['date'], df['likes'], marker='o', color='green')
        axes[2].set_title('Likes')

        # 4. CTR
        axes[3].plot(df['date'], df['ctr'] * 100, marker='o', color='red')
        axes[3].set_title('CTR (%)')

        # 5. Messages
        axes[4].plot(df['date'], df['messages_sent'], marker='o', color='purple')
        axes[4].set_title('Messages sent')
        axes[4].ticklabel_format(style='plain', useOffset=False, axis='y')
        axes[4].yaxis.set_major_formatter(StrMethodFormatter('{x:.0f}'))
       
        plt.tight_layout(rect=[0, 0, 1, 0.97])

        # header
        fig.suptitle('Key application metrics for the last 7 days', fontsize=14)

        for ax in axes:
            ax.tick_params(axis='x', rotation=45)
            ax.grid(alpha=0.5)

        # Remove the empty cell (6).
        fig.delaxes(axes[5])

        plt.tight_layout(rect=[0, 0, 1, 0.97])

        # save&send
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'core_metrics_7days.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    @task
    def feed_and_sms_report_weekly_dau():
        chat_id = 427641111
        my_token = '1111111111:AAFHHxLQAz3O6B49fZnvp6v11PDYo6aTzVQ'
        bot = telegram.Bot(token=my_token)
                
        core_metrics_graf = Getch("""
         SELECT 
            toDate(time) AS date,
            uniqExact(user_id) AS dau,
            'feed' AS source
        FROM simulator_20250820.feed_actions
        WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
        GROUP BY date

        UNION ALL

        SELECT 
            toDate(time) AS date,
            uniqExact(user_id) AS dau,
            'messages' AS source
        FROM simulator_20250820.message_actions
        WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
        GROUP BY date

        UNION ALL

        SELECT 
            toDate(time) AS date,
            uniqExact(user_id) AS dau,
            'total' AS source
        FROM (
            SELECT user_id, time FROM simulator_20250820.feed_actions
            UNION ALL
            SELECT user_id, time FROM simulator_20250820.message_actions
        )
        WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
        GROUP BY date
        ORDER BY date, source
        """).df
        
        df_feed = core_metrics_graf[core_metrics_graf['source'] == 'feed']
        df_messages = core_metrics_graf[core_metrics_graf['source'] == 'messages']

        # create two subplots
        fig, axes = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

        # top - news feed
        axes[0].plot(df_feed['date'], df_feed['dau'], marker='o', color='royalblue')
        axes[0].set_title('DAU news feed')
        axes[0].set_ylabel('Users')
        axes[0].grid(alpha=0.5)

        # bottom - messages
        axes[1].plot(df_messages['date'], df_messages['dau'], marker='o', color='orange')
        axes[1].set_title('DAU messanger')
        axes[1].set_xlabel('Date')
        axes[1].set_ylabel('Users')
        axes[1].grid(alpha=0.5)

        plt.suptitle('DAU of the news feed and messenger over the last 7 days', fontsize=13)
        plt.xticks(rotation=45)
        plt.tight_layout(rect=[0, 0, 1, 0.96])

        # save&send
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'dau_feed_and_messages.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    @task
    def feed_cohort_report():
        chat_id = 427641111
        my_token = '1111111111:AAFHHxLQAz3O6B49fZnvp6v11PDYo6aTzVQ'
        bot = telegram.Bot(token=my_token)
        
        core_metrics_graf = Getch("""
        select
            this_week, 
            previous_week, 
            -uniq(user_id) as num_users,
            status
        from
        (select 
            user_id,
            groupUniqArray(toMonday(toDate(time))) as weeks_visited,
            addWeeks(arrayJoin(weeks_visited), +1) as this_week,
            if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status,
            addWeeks(this_week, -1) as previous_week
        from simulator_20250820.feed_actions
        group by user_id
        )
        where status = 'gone'
        group by this_week, previous_week, status
        having this_week != addWeeks(toMonday(today()), +1)

        union all

        select 
            this_week, 
            previous_week, 
            toInt64(uniq(user_id)) as num_users,
            status
        from
        (select user_id,
            groupUniqArray(toMonday(toDate(time))) as weeks_visited,
            arrayJoin(weeks_visited) as this_week,
            if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status,
            addWeeks(this_week, -1) as previous_week
        from simulator_20250820.feed_actions
        group by user_id
        )
        group by this_week, previous_week, status
        order by this_week, status
        """).df
        
        core_metrics_graf['this_week'] = pd.to_datetime(core_metrics_graf['this_week'])

        # Each week is a row, each status is a column
        pv = core_metrics_graf.pivot_table(
            index='this_week',
            columns='status',
            values='num_users',
            aggfunc='sum'
        ).fillna(0).sort_index()

        # Values for 'gone' are negative - bars go downward
        if 'gone' in pv.columns:
            pv['gone'] = -pv['gone'].abs()

        fig, ax = plt.subplots(figsize=(12, 6))

        ax.bar(pv.index, pv.get('new', 0), label='new', color='tomato')
        ax.bar(pv.index, pv.get('retained', 0), bottom=pv.get('new', 0), label='retained', color='lightgreen')
        ax.bar(pv.index, pv.get('gone', 0), label='gone', color='royalblue')

        ax.axhline(0, color='black', linewidth=1)
        ax.grid(alpha=0.5)
        ax.legend(title='Status')

        ax.set_title('Weekly cohort analysis of news feed users', fontsize=13)
        ax.set_xlabel('Week')
        ax.set_ylabel('Number of users')
        plt.xticks(rotation=45)
        plt.tight_layout()

        # save&send
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'cohort_analysis.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    text_report = general_text_report()
    weekly_report = feed_and_sms_report_weekly()
    weekly_dau_report = feed_and_sms_report_weekly_dau()
    cohort_report = feed_cohort_report()

    text_report >> weekly_report >> weekly_dau_report >> cohort_report

dag_aleksandr_antonov_hnm5755_bot_advanced = dag_aleksandr_antonov_hnm5755_bot_advanced()