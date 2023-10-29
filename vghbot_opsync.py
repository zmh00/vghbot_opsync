from vghbot_kit import vghbot_login, gsheet
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from io import StringIO
import random
import logging

def schedule_get(doc):
    '''
    回傳今天以後的未處理手術排程df+response.text
    傳入參數: 醫師燈號(4102)
    '''
    url = 'https://web9.vghtpe.gov.tw/ops/opb.cfm'
    payload_doc = {
        'action': 'findOpblist',
        'type': 'opbmain',
        'qry': doc, # '4102',
        'bgndt': str(datetime.today().year - 1911) + (datetime.today()+SEARCH_OFFSET).strftime("%m%d"), # '1120703',
        '_': int(time.time()*1000)
    }
    response = webclient.session.get(url, params=payload_doc)
    df = pd.read_html(StringIO(response.text), flavor='lxml')[0]
    df = df.astype('string')

    return df, response.text


def schedule_process(df, response_text):
    soup = BeautifulSoup(response_text, "html.parser")

    # TODO 清除已取消的排程
    
    # 透過該刀表的column name順序來重新排序資料
    link_list = soup.find_all('button', attrs={'data-target':"#myModal"})
    df['link'] = [l['data-url'] for l in link_list]

    tooltip_list = soup.find_all('a', attrs={'data-toggle':"tooltip"})
    df['tooltip'] = [l['title'] for l in tooltip_list]
    df = pd.concat([df, df['tooltip'].str.extract(r'術前診斷:\s*(?P<診斷>.*?)\s*手術名稱:\s*(?P<手術>.*?)\s*手術室資訊:\s*(?P<備註>.*?)\s*麻醉:\s*(?P<麻醉>.*?)\s*$')], axis=1 )
    df['側別'] = df['診斷'].str.extract(r'\s([Oo][Dd]|[Oo][Ss]|[Oo][Uu])')
    df['側別'] = df['側別'].str.upper()

    formatted_df = df[['手術日期', '手術時間', '姓名', '病歷號', '診斷', '側別', '手術', '備註', '麻醉','病房床號', '開刀房號', '狀態']]
    return formatted_df


def gsheet_acc(dr_code: str):
    '''
    Input: short code of account. Ex:4123
    Output: return dictionary of {'ACCOUNT':...,'PASSWORD':...,'NAME':...} 
    '''
    df = gc.get_df(gsheet.GSHEET_SPREADSHEET, gsheet.GSHEET_WORKSHEET_ACC)
    dr_code = str(dr_code).upper()
    selector = df['ACCOUNT'].str.contains(dr_code, case = False)
    selected_df = df.loc[selector,:]
    if len(selected_df) == 0: # 資料庫中沒有此帳密
        logger.error(f"USER({dr_code}) NOT EXIST IN CONFIG")
        return None, None
        
    elif len(selected_df) > 1:
        logger.error(f"MORE THAN ONE RECORD: {dr_code}, WILL USE THE FIRST ONE")

    result = selected_df.iloc[0,:].to_dict() #df變成series再輸出成dict
    return result['ACCOUNT'], result['PASSWORD']


# Initialization
gc = gsheet.GsheetClient()
config = gc.get_col_dict(gsheet.GSHEET_SPREADSHEET, gsheet.GSHEET_WORKSHEET_OPSYNC)
for c in config: # 將list格式去掉
    if len(config[c]) == 1:
        config[c] = config[c][0]

login_id, login_psw = gsheet_acc(config.get('LOGIN_DOC'))
webclient = vghbot_login.Client(login_id=login_id, login_psw=login_psw)
webclient.login_drweb()

# Logging 設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # 這是logger的level
BASIC_FORMAT = '[%(asctime)s %(levelname)-8s] %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(BASIC_FORMAT, datefmt=DATE_FORMAT)
# 設定file handler的設定
log_filename = "opsync.log"
fh = logging.FileHandler(log_filename)  # 預設mode='a'，持續寫入
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

# 設定console handler的設定 # TODO 可移除
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  # 可以獨立設定console handler的level，如果不設就會預設使用logger的level
ch.setFormatter(formatter)

# 將handler裝上
logger.addHandler(ch) # TODO 可移除
logger.addHandler(fh)

WORKSHEET_SYNC = config.get('WORKSHEET_SYNC')
WORKING_START = datetime.strptime(config.get('WORKING_START'), '%H:%M').time()
WORKING_END = datetime.strptime(config.get('WORKING_END'), '%H:%M').time()
SEARCH_OFFSET = timedelta(int(config.get('SEARCH_OFFSET')))
DEFAULT_SYMBOL = config.get('DEFAULT_SYMBOL')

old_indexes = []
old_df = dict()


def main():
    global old_df, old_indexes
    while True:
        try:
            now = datetime.today().time()
            if WORKING_START <= now <= WORKING_END:
                INTERVAL = int(config.get('WORKING_INTERVAL'))
            else:
                INTERVAL = int(config.get('RESTING_INTERVAL'))

            INDEXES = config.get('INDEXES') # INDEXES 統一使用list型態
            if type(INDEXES) != list:
                INDEXES = [INDEXES]
            
            if INDEXES != old_indexes:
                df_surgery = gc.get_df(gsheet.GSHEET_SPREADSHEET, gsheet.GSHEET_WORKSHEET_SURGERY) # 讀取index對應的config
            # get specified indexes from gsheet
            
            # iterate through each index
            for index in INDEXES:
                index = index.strip()
                if index == '':
                    continue
                config_surgery = (df_surgery
                                  .loc[(df_surgery['INDEX']==index)|(df_surgery['INDEX']==DEFAULT_SYMBOL),:] # 匹配相同與DEFAULT
                                  .sort_values(by=['INDEX'], axis=0) # DEFAULT排序較後面
                                  .to_dict('records')[0] # filter出來是dataframe格式
                                  ) 
                ssheet = gc.client.open(config_surgery['SPREADSHEET'])
                
                # check if the worksheet is already created
                new = True
                for wsheet in ssheet: 
                    if wsheet.title == WORKSHEET_SYNC:
                        new = False
                        break
                if new:
                    wsheet = ssheet.add_worksheet(title=WORKSHEET_SYNC) # create one if not exist
                else:
                    wsheet = ssheet.worksheet_by_title(WORKSHEET_SYNC)

                raw_df, response_text = schedule_get(index)
                update_time = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

                
                if raw_df.equals(old_df.get(index)): # 如果跟上次相同就continue
                    logger.info(f'{datetime.today()}|No change for {index}')
                    continue
                else: # 如果有差異
                    df = schedule_process(raw_df.copy(), response_text)
                    wsheet.update_value('A1', f"更新時間:{update_time}")
                    wsheet.set_dataframe(df, 'A2', copy_index=False, nan='')
                    logger.info(f'{datetime.today()}|Sync:{index}')
                    old_df[index] = raw_df # 存入做下次比較
            old_indexes = INDEXES
        except Exception as e:
            logger.error(e)
        finally:
            logger.info(f"WAITING INTERVAL: {INTERVAL}")
            time.sleep(INTERVAL + random.randint(0, int(INTERVAL/10)))

if __name__ == '__main__':
    main()

# pyinstaller -F vghbot_opsync.py

# ini file
# [DEFAULT]
# LOGIN_ID = ***
# LOGIN_PSW = ***
# WORKING_INTERVAL = 180
# WORKING_START = 08:30
# WORKING_END = 20:30
# RESTING_INTERVAL = 1800
# INDEXES = 4066