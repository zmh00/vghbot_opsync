from vghbot_kit import vghbot_login, gsheet
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from io import StringIO
import random
import logging

def schedule_get(webclient, doc, search_offset):
    '''
    回傳今天以後的未處理手術排程df+response.text
    傳入參數: 醫師燈號(4102)
    '''
    url = 'https://web9.vghtpe.gov.tw/ops/opb.cfm'
    payload_doc = {
        'action': 'findOpblist',
        'type': 'opbmain',
        'qry': doc, # '4102',
        'bgndt': str(datetime.today().year - 1911) + (datetime.today()+search_offset).strftime("%m%d"), # '1120703',
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
    
    df = df.assign(
        LenSx = '',
        IOL = '',
        Final = '',
        SN = '',
        Complications = ''
    )
    # 側別: 利用診斷內部的ODOS標記判斷側別 # TODO未來考慮更細緻的判斷，雙眼做不同手術的那種
    df['側別'] = df['診斷'].str.extract(r'\s([Oo][Dd]|[Oo][Ss]|[Oo][Uu])')
    df['側別'] = df['側別'].str.upper()

    try:    
        # Lensx
        df.loc[df['手術'].str.contains('lensx', case=False), 'LenSx'] = 'LenSx' # 按照手術找是否有lensx
    except:
        logger.error("Regex error: Lensx")
        
    try:
        # IOL
        df['IOL'] = df['手術'].str.extract(r"IOL.*\(\s*([\w\d]+(?=\s|(?=\+)))") # 匹配字串"IOL"後方且"("後方擷取文字包含數字，且要在後方的空格之前或是後方的"+"之前
    except:
        logger.error("Regex error: IOL")
    
    try:
        # Final
        df['Final'] = df['手術'].str.extract(r"(?<!T:)(?<!T)(?<!t)(?<!t:)([+-]+\d+\.\d+)") # 匹配後方有+/-號開頭的數字，且需要有小數點，且前方不該出現T:/T/t:/t
    except:
        logger.error("Regex error: Final")
        
    formatted_df = df[['手術日期', '手術時間', '姓名', '病歷號', '診斷', '手術', '側別', 'LenSx', 'IOL', 'Final', 'SN',	'Complications', '備註', '開刀房號', '麻醉','病房床號', '狀態']]
    return formatted_df


def gsheet_acc(gc, dr_code: str):
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



old_indexes = []
old_df = dict()

# TODO 因為每個擷取能設定獨立的啟動終止時間和interval，應該要設計成多線程，每個是獨立的，目前是共用最後一個interval

def main():
    global old_df, old_indexes
    while True:
        try:
            # Initialization every cycle，先載入config和欲登入查詢的燈號
            gc = gsheet.GsheetClient()
            config = gc.get_col_dict(gsheet.GSHEET_SPREADSHEET, gsheet.GSHEET_WORKSHEET_CONFIG)
            login_id, login_psw = gsheet_acc(gc, config.get('OPSYNC_LOGIN_DOC')[0])
            webclient = vghbot_login.Client(login_id=login_id, login_psw=login_psw)
            webclient.login_drweb()
            
            # 載入opsync清單
            sync_df = gc.get_df(gsheet.GSHEET_SPREADSHEET, gsheet.GSHEET_WORKSHEET_OPSYNC)

            # iterate each row of the config and do ...
            for row in sync_df.itertuples(index=True, name='Pandas'):
                # read every row data of opsync
                INDEX = row.INDEX.strip()
                if INDEX == '': # 跳過空列
                    continue
                
                SPREADSHEET_SYNC = row.SPREADSHEET_SYNC
                WORKSHEET_SYNC = row.WORKSHEET_SYNC
                WORKING_START = datetime.strptime(row.WORKING_START, '%H:%M').time()
                WORKING_END = datetime.strptime(row.WORKING_END, '%H:%M').time()
                SEARCH_OFFSET = timedelta(int(row.SEARCH_OFFSET))
                DEFAULT_SYMBOL = row.DEFAULT_SYMBOL # TODO remove

                now = datetime.today().time()
                if WORKING_START <= now <= WORKING_END:
                    INTERVAL = int(row.WORKING_INTERVAL)
                else:
                    INTERVAL = int(row.RESTING_INTERVAL)

                # Main
                ssheet = gc.client.open(SPREADSHEET_SYNC)
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

                # 擷取手術排成資料
                raw_df, response_text = schedule_get(webclient, INDEX, SEARCH_OFFSET)
                update_time = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

                
                if raw_df.equals(old_df.get(INDEX)): # 如果跟上次相同就continue
                    logger.info(f'{datetime.today()}|No change for {INDEX}')
                    continue
                else: # 如果有差異
                    df = schedule_process(raw_df.copy(), response_text)
                    wsheet.update_value('A1', f"更新時間:{update_time}")
                    wsheet.set_dataframe(df, 'B1', copy_index=False, nan='')
                    logger.info(f'{datetime.today()}|Sync:{INDEX}')
                    old_df[INDEX] = raw_df # 存入做下次比較

        except Exception as e:
            logger.error(e)
        finally:
            logger.info(f"WAITING INTERVAL: {INTERVAL}")
            time.sleep(INTERVAL + random.randint(0, int(INTERVAL/10)))

if __name__ == '__main__':
    main()

# pyinstaller -F vghbot_opsync.py