from vghbot_kit import vghbot_login, gsheet
import configparser, time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from io import StringIO
import random

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
        'bgndt': str(datetime.today().year - 1911) + datetime.today().strftime("%m%d"), # '1120703',
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

config = configparser.ConfigParser()
config.read('opsync.ini', encoding='utf-8')

gc = gsheet.GsheetClient()
webclient = vghbot_login.Client(login_id=config['DEFAULT'].get('login_id'), login_psw=config['DEFAULT'].get('login_psw')) # TODO 未來替換成查雲端表
webclient.login_drweb()


WORKSHEET_SYNC = 'sync'
OLD_INDEXES = []
OLD_DF = dict()
WORKING_START = datetime.strptime(config['DEFAULT'].get('working_start'), '%H:%M').time()
WORKING_END = datetime.strptime(config['DEFAULT'].get('working_end'), '%H:%M').time()

# ini file
# [DEFAULT]
# LOGIN_ID = ***
# LOGIN_PSW = ***
# WORKING_INTERVAL = 180
# WORKING_START = 08:30
# WORKING_END = 20:30
# RESTING_INTERVAL = 1800
# INDEXES = 4066

while True:
    try:
        now = datetime.today().time()
        if WORKING_START <= now <= WORKING_END:
            INTERVAL = config['DEFAULT'].getint('working_interval')
        else:
            INTERVAL = config['DEFAULT'].getint('resting_interval')
        
        print(f"WAITING INTERVAL: {INTERVAL}")

        INDEXES = config['DEFAULT'].get('indexes').split(',')
        if INDEXES != OLD_INDEXES:
            df_surgery = gc.get_df(gsheet.GSHEET_SPREADSHEET, gsheet.GSHEET_WORKSHEET_SURGERY) # 讀取index對應的config
        # get specified indexes from gsheet
        
        # iterate through each index
        for index in INDEXES:
            index = index.strip()
            config_surgery = df_surgery.loc[df_surgery['INDEX']==index,:].to_dict('records')[0] # FIXME 這是series還是dataframe to_dict??
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
            
            if raw_df.equals(OLD_DF.get(index)): # 如果跟上次相同就continue
                print(f'{datetime.today()}|No change for {index}')
                continue
            else: # 如果有差異
                df = schedule_process(raw_df.copy(), response_text)
                wsheet.set_dataframe(df, 'A1', copy_index=False, nan='')
                print(f'{datetime.today()}|Sync ONCE')
                OLD_DF[index] = raw_df # 存入做下次比較
        OLD_INDEXES = INDEXES
    except Exception as e:
        print(e)
    finally:
        time.sleep(INTERVAL + random.randint(0, int(INTERVAL/10)))