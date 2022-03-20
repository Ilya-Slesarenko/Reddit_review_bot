# my credentials come with lock.stock gmail account
# initial article source: https://towardsdatascience.com/scraping-reddit-data-1c0af3040768
# this version  will catch_only the needed content from all lines!

# -*- coding: utf-8 -*-
import logging, random, warnings, httplib2, apiclient.discovery, praw, re
from datetime import datetime, timedelta, time
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from googletrans import Translator  # python -m pip install googletrans==4.0.0-rc1

warnings.filterwarnings('ignore')


class RedditInfoGetter():
    def __init__(self):
        self.my_id = 'ZdwaLgvPj4v57wqAziZeQg'
        self.my_secret = 'X-qQvqWkG55k6s1vTdpS3EnyuHDlYQ'
        self.my_agent = 'parse_news'
        self.rubrics_we_search = ['stock', 'dataisbeautiful']
        self.catch_context = ['stock', 'Stock', 'market', 'Market', 'rich', 'Rich']
        
        CREDENTIALS_FILE = 'stock-spreadsheets-9974a749b7e4.json'
        self.reddit_data_page = '1PzQreY1EmcVg1_xu6ThpmcL3CTEmEHedoykN34OEugE'
        credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
        httpAuth = credentials.authorize(httplib2.Http())  # Авторизуемся в системе
        self.service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)  # Выбираем работу с таблицами и 4 версию API


    def G_Sheet_filling(self, dataframe, link_g_sheet, sheet_title, columns_range):
        # working with the insiders deals page - first, reading the current data to clear them up
        report_page_data = self.service.spreadsheets().values().get(spreadsheetId=link_g_sheet, range=f'{sheet_title}!{columns_range}', valueRenderOption='FORMATTED_VALUE', dateTimeRenderOption='FORMATTED_STRING').execute()
        # report_page_data = service.spreadsheets().values().get(spreadsheetId=link_g_sheet, range=f'{sheet_title}!{columns_range}', valueRenderOption='FORMATTED_VALUE', dateTimeRenderOption='FORMATTED_STRING').execute()

        sheet_data = report_page_data.get("values")
        headers = report_page_data.get("values")[0]

        # clear_data
        clear_up_range = []  # выбираем заполненные значения, определяем нулевую матрицу для обнуления страницы
        for _ in sheet_data:  # число строк с текущим заполнением
            clear_up_range.append([str('')] * len(headers))
        # print(f'\n\n{len(clear_up_range)}\n\n{len(clear_up_range[1])}\n\n')

        null_matrix = self.service.spreadsheets().values().batchUpdate(spreadsheetId=link_g_sheet, body={
            "valueInputOption": "USER_ENTERED",
            "data": [{"range": f'{sheet_title}!{columns_range}',
                      "majorDimension": "ROWS",
                      "values": clear_up_range}]
        }).execute()

        # making appropriate range from the new dataframe
        new_data_values = dataframe.values.tolist()
        new_d = [headers]
        for i in new_data_values:
            new_line = [str(t) if type(t) == time else t for t in i]
            new_d.append(new_line)

        new_data_filling = self.service.spreadsheets().values().batchUpdate(spreadsheetId=link_g_sheet, body={
            "valueInputOption": "USER_ENTERED",
            "data": [{"range": f'{sheet_title}!{columns_range}',
                      "majorDimension": "ROWS",
                      "values": new_d}]
        }).execute()


    def reddit_all_types(self, rubric_list, news_lim, time_delta):
        reddit = praw.Reddit(client_id=self.my_id, client_secret=self.my_secret, user_agent=self.my_agent)
        # reddit = praw.Reddit(client_id=my_id, client_secret=my_secret, user_agent=my_agent)

        posts = []
        posts_list_check = []  # чтобы проверить посторы общим списком, без подписей класса новостей

        for rubric in rubric_list:
            # get 10 hot posts from the MachineLearning subreddit  # set 'all' for all kinds of news if needed
            # can set to 'hot' or 'new' or 'rising' etc. check the Reddit types!
            hot_posts = reddit.subreddit(f'{rubric}').hot(limit=news_lim)
            new_posts = reddit.subreddit(f'{rubric}').new(limit=news_lim)
            top_posts = reddit.subreddit(f'{rubric}').top(limit=news_lim)
            rising_posts = reddit.subreddit(f'{rubric}').rising(limit=news_lim)

            for post in hot_posts:
                created_utc = post.created
                parsed_date = datetime.utcfromtimestamp(created_utc)
                title = post.title
                if parsed_date >= datetime.now() - timedelta(time_delta) and any(re.search(i, title) for i in self.catch_context):
                    score, url, num_comments, context, year, month, day, time = post.score, post.url, post.num_comments, post.selftext, parsed_date.year, parsed_date.month, parsed_date.day, parsed_date.time()
                    translated_title = Translator().translate(str(title), src='en', dest='ru').text
                    new_line = ['hot', rubric, title, translated_title, score, url, num_comments, context, year, month, day, time]
                    new_check_line = [title, context, year, month, day, time]
                    if new_check_line not in posts_list_check:
                        posts_list_check.append(new_check_line)
                        posts.append(new_line)
            for post in new_posts:
                created_utc = post.created
                parsed_date = datetime.utcfromtimestamp(created_utc)
                title = post.title
                if parsed_date >= datetime.now() - timedelta(time_delta) and any(re.search(i, title) for i in self.catch_context):
                    score, url, num_comments, context, year, month, day, time = post.score, post.url, post.num_comments, post.selftext, parsed_date.year, parsed_date.month, parsed_date.day, parsed_date.time()
                    translated_title = Translator().translate(str(title), src='en', dest='ru').text
                    new_line = ['hot', rubric, title, translated_title, score, url, num_comments, context, year, month,day, time]
                    new_check_line = [title, context, year, month, day, time]
                    if new_check_line not in posts_list_check:
                        posts_list_check.append(new_check_line)
                        posts.append(new_line)
            for post in top_posts:
                created_utc = post.created
                parsed_date = datetime.utcfromtimestamp(created_utc)
                title = post.title
                if parsed_date >= datetime.now() - timedelta(time_delta) and any(re.search(i, title) for i in self.catch_context):
                    score, url, num_comments, context, year, month, day, time = post.score, post.url, post.num_comments, post.selftext, parsed_date.year, parsed_date.month, parsed_date.day, parsed_date.time()
                    translated_title = Translator().translate(str(title), src='en', dest='ru').text
                    new_line = ['hot', rubric, title, translated_title, score, url, num_comments, context, year, month,day, time]
                    new_check_line = [title, context, year, month, day, time]
                    if new_check_line not in posts_list_check:
                        posts_list_check.append(new_check_line)
                        posts.append(new_line)
            for post in rising_posts:
                created_utc = post.created
                parsed_date = datetime.utcfromtimestamp(created_utc)
                title = post.title
                if parsed_date >= datetime.now() - timedelta(time_delta) and any(re.search(i, title) for i in self.catch_context):
                    score, url, num_comments, context, year, month, day, time = post.score, post.url, post.num_comments, post.selftext, parsed_date.year, parsed_date.month, parsed_date.day, parsed_date.time()
                    translated_title = Translator().translate(str(title), src='en', dest='ru').text
                    new_line = ['hot', rubric, title, translated_title, score, url, num_comments, context, year, month,day, time]
                    new_check_line = [title, context, year, month, day, time]
                    if new_check_line not in posts_list_check:
                        posts_list_check.append(new_check_line)
                        posts.append(new_line)

            print(f'''Rubric '{rubric}' checked.''')

        headers = ['class', 'rubric', 'title', 'translated_title', 'score', 'url', 'num_comments', 'body', 'yr', 'mo','dd','time']
        posts_df = pd.DataFrame(posts,columns=headers)
        self.G_Sheet_filling(posts_df, self.reddit_data_page, 'catch', 'A:L')


    def PerformAll(self):
        self.reddit_all_types(self.rubrics_we_search, 100, 150) # taking top 100 posts from each class, within 150 days prior to today
        print('We\'re all set!')


if __name__ == '__main__':
    RedditInfoGetter().PerformAll()