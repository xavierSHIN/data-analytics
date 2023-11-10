import sqlalchemy, urllib
import re, os, json, shutil
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime
import random

CONFIG_PATH = r'C:\Users\dongx\PycharmProjects\SG-data\luffy\config'
filePath = r'C:\Users\dongx\PycharmProjects\SG-data\luffy'


def parse_wan(input_str):
    input_str = str(input_str)
    if re.search('.*(?=万)', input_str) is not None:
        whole = re.search('.*(?=万)', input_str)[0]
        numb = float(whole) * 10000
    else:
        numb = float(input_str)
    return numb


# connect to a database
# dbConnection = 'xu_mysql'
def db(dbConnection):
    # dbConnection = "pgWarehouse"
    try:
        with open(os.path.join(CONFIG_PATH, "db_creds.json")) as dbFile:
            dbCreds = json.load(dbFile)[dbConnection]
    except:
        dbCreds = {"dialect": "", "user": "", "pw": "", "host": "", "db": ""}

    if dbCreds["dialect"] == "mssql+pyodbc":  # check for dialect here instead of specific connection
        odbcString = urllib.parse.quote_plus("driver={0};server={1};port={2};database={3};uid={4};pwd={5}".format(
            dbCreds["driver"]
            , dbCreds["host"]
            , dbCreds["port"]
            , dbCreds["db"]
            , dbCreds["user"]
            , dbCreds["pw"]
        ))
        dbString = "{0}:///?odbc_connect={1}".format(dbCreds["dialect"], odbcString)
    else:
        try:
            dbString = "{0}://{1}:{2}@{3}/{4}?charset={5}".format(dbCreds["dialect"],
                                                    urllib.parse.quote_plus(dbCreds["user"]),
                                                      urllib.parse.quote_plus(dbCreds["pw"]),
                                                                  dbCreds["host"],
                                                      dbCreds["db"]
                                                                  , dbCreds["encode"])
        except:
            dbString = ""

    try:
        dbEngine = sqlalchemy.create_engine(dbString, pool_pre_ping=True)
    except:
        dbEngine = None

    return dbEngine
# end def db()


def log_batch_id(
        batchType
        , warehouseEngine=None
):
    if not warehouseEngine:
        warehouseEngine = db("pgWarehouse")
    try:
        batchId = int(pd.read_sql("SELECT nextval('meta.{0}_id') {0}_id".format(batchType), warehouseEngine)[
                          "{0}_id".format(batchType)][0])
    except:
        batchId = None
    return batchId


# end def log_batch_id()


# to_database func
def import_file_to_table(
    importData
    , importTable
    , fileName
    , fileTime
    , pgEngine = None
    , importId = None
    , filePath = ""
    , dedupeFields = [] #
):
    importSuccess = False

    if pgEngine == None:
        pgEngine = db("pgWarehouse")

    if importId == None:
        importId = log_batch_id("import", pgEngine)

    logParams = {"importId": importId, "importTable": importTable, "fileName": fileName, "fileTime": fileTime.strftime(format = "%Y-%m-%d %H:%M:%S%z"), "filePath": filePath}
    fileId = int(pd.read_sql(sqlalchemy.text("""
        INSERT INTO meta.log_file_import
        (import_id, import_table, file_name, file_time, file_path, import_start)
        VALUES
        (:importId, :importTable, :fileName, :fileTime, :filePath, now())
        RETURNING file_id
        ;
    """), con = pgEngine, params = logParams)["file_id"][0])

    metadataFields = pd.read_sql(sqlalchemy.text("""
        SELECT file_name_field
            , file_time_field
        FROM meta.import_file_name_field
        WHERE import_table = :importTable
    """), con = pgEngine, params = {"importTable": importTable})

    fileNameField = "file_name" if metadataFields.file_name_field.empty else metadataFields.file_name_field[0]
    fileTimeField = "file_time" if metadataFields.file_time_field.empty else metadataFields.file_time_field[0]

    if fileNameField not in importData.columns:
        importData[fileNameField] = fileName

    if fileTimeField not in importData.columns:
        importData[fileTimeField] = fileTime

    # no need to check if importTable exists; if it doesn't, this will be an empty list:
    existingColumns = pd.read_sql(sqlalchemy.text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'imports'
            AND table_name = :importTable
    """), con = pgEngine, params = {"importTable": importTable}).column_name.tolist()
    # if existingCols are empty, importCols are empty; newCols capture all new cols
    importColumns = [element for element in existingColumns if element in importData.columns]
    newCols = [element for element in importData.columns if element not in existingColumns]
    importColumns.extend(newCols)
    importData = importData[importColumns]
    if len(existingColumns) != 0: # keep the dtypes of 1st time imported table
        for newCol in newCols:
            importData[newCol] = importData[newCol].astype(str)
    pgConnection = pgEngine.connect()
    pgTransaction = pgConnection.begin()

    try:
        pgConnection.execute("SET ROLE superagent;")

        # add any new columns as text type (except missing file time fields, which are timestamptz)
        if len(existingColumns) != 0:
            for newCol in newCols:
                pgConnection.execute(sqlalchemy.text("""
                ALTER TABLE imports.{0} ADD COLUMN {1} {2}
            """.format(importTable, pgEngine.dialect.identifier_preparer.quote(newCol), "timestamptz" if newCol == fileTimeField else "text")))

        # dedupe before import if necessary (TODO: consolidate dedupe processes from various import jobs)
        dedupeRows = None
        if dedupeFields:
            dedupeRows = pgConnection.execute(sqlalchemy.text("""
                DELETE FROM imports.{0} WHERE ({1}) IN :dedupeValues
            """.format(importTable, ", ".join([pgEngine.dialect.identifier_preparer.quote(dedupeField) for dedupeField in dedupeFields])))
                , dedupeValues = tuple([tuple(dedupeValue) for dedupeValue in importData[dedupeFields].values])
            ).rowcount

        importData.to_sql(name = importTable, con = pgConnection, index = False, schema = "imports", if_exists = "append")
        pgConnection.execute("RESET ROLE;")

    except Exception as e:
        print(e)
        if sqlalchemy.exc.DataError:
            dtypeExisting = pd.read_sql(sqlalchemy.text("""
                SELECT column_name, data_type data_type_existing FROM information_schema.columns WHERE table_schema = 'imports' AND table_name = :importTable
            """), con = pgConnection, params = {"importTable": importTable})
            dtypeSet = importData.dtypes
            dtypeImport = pd.DataFrame({'column_name': dtypeSet.index, 'data_type_import': dtypeSet.values})
            comparison = pd.merge(dtypeExisting, dtypeImport, on = 'column_name', how = 'left')
            for col in ['data_type_existing', 'data_type_import']:
                comparison[col] = comparison[col].astype('str')
            careList = {}
            for index, row in comparison.iterrows():
                if row.data_type_existing in ['integer', 'bigint', 'double precision'] and row.data_type_import == 'object':
                    careList.update({row.column_name: (row.data_type_existing, type(importData.loc[0, row.column_name]))})
                if row.data_type_existing in ['timestamp without time zone', 'timestamp with time zone'] and row.data_type_import == 'object':
                    careList.update({row.column_name: (row.data_type_existing, type(importData.loc[0, row.column_name]))})
            print("Data-type errors: " + str(careList))
        else:
            print('Unknown error when writing to import table')
        pgTransaction.rollback()
        print('Rolling back import')

    else: # no error on import
        try:
            importRows = int(pd.read_sql(sql = sqlalchemy.text("""
                SELECT count(*) import_rows FROM imports.{0} WHERE {1} = :fileName
            """.format(importTable, fileNameField)), con = pgConnection, params = {"fileName": fileName})["import_rows"][0])
        except Exception as e:
            print(e)
            if sqlalchemy.exc.ProgrammingError:
                if fileNameField not in importData.columns.tolist():
                    print('Import data is missing a file-name column')
            importRows = 0

        print("{0} rows imported, {1} expected, {2}".format(
            importRows
            , len(importData.index)
            , "no dedupe attempted" if dedupeRows is None else "{0} rows deduped".format(dedupeRows)
        )) # include dedupe info here

        if importRows == len(importData.index):
            pgConnection.execute(sqlalchemy.text("""
                UPDATE meta.log_file_import
                SET import_end = clock_timestamp()
                    , import_rows = :importRows
                WHERE file_id = :fileId
                ;
            """), importRows = importRows, fileId = fileId)
            pgTransaction.commit()
            importSuccess = True
            print("Committing successful import")
        else:
            pgTransaction.rollback()
            print("Rolling back failed import")

    finally:
        pgConnection.close()

    return importSuccess
# end def import_file_to_table()


def snake_case(camelCase):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camelCase)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    s3 = re.sub("\W", "_", s2)
    return re.sub("__+","_",s3)
# end def snake_case()

# Python code to get the Cumulative sum of a list
#ls = [5,4,3,2,1]
#for i in range(0, len(ls)+1):
#    print(ls[0:i:1])
#df = pd.DataFrame({'xixi': ls})
#df['dada'] = df['xixi'].apply(lambda x: sum(
#    df['xixi'][0:df['xixi'].tolist().index(x)+1]))
# type()
# df['che'] =  df[['xixi']].apply(np.cumsum)

def Cumulative(lists):

	cu_list = []
	length = len(lists)
	cu_list = [sum(lists[0:x:1]) for x in range(0, length+1)]

	return cu_list[1:]


def move_file():

    picsPath = r'C:\Users\dongx\Pictures\part I'
    targetPath = r'C:\Users\dongx\Pictures\PNG'
    xixi = os.listdir(picsPath)
    errorlist = []
    tailList = []

    for item in xixi:
        extension = re.search('(?<=\.)(.*$)', str(item))[0]
        tailList.append(extension)
        if extension not in ('JPG'):
            errorlist.append(item)
            print(item)
            shutil.move(os.path.join(picsPath, item), os.path.join(targetPath, item))

    return


def generate_session_data():

    repK = [150, 100, 50, 30, 10, 3, 1]
    barK = [50, 30, 10, 10, 10, 70, 90]
    userList = np.random.randint(10000, 20000, size=sum(barK) * 2)
    userSample = []
    for item in userList:
        if len(set(userSample)) < sum(barK):
            if item not in userSample:
                userSample.append(item)
        else:
            break
    daList = []
    dateList = []
    actionList = []
    actionV = ['acquire', 'activation', 'retention', 'revenue', 'refer']
    step = 0
    for k, v in dict(zip(barK, repK)).items():
        alist = userSample[step: step + k] * v
        blist = random.choices(range(60), k=k) * v
        clist = [actionV[j] for j in random.choices(range(5), k=k)] * v
        daList.extend(alist)
        dateList.extend(blist)
        actionList.extend(clist)
        step += k
        alist = []
        blist = []
        clist = []
    # len(dateList), len(daList), len(actionList)
    sessionList = random.choices(range(1000000, 2000000), k=len(daList))
    dateList.sort()
    bData = pd.DataFrame({'user_id': daList, 'session_id': sessionList, 'view_date': dateList, 'action': actionList})
    bData['view_date'] = bData['view_date'].apply(lambda x: datetime.date(2023, 1, 1) + datetime.timedelta(x))
    for col in ['user_id', 'session_id']:
        bData[col] = bData[col].astype(str)

    return bData


#set(tailList)
#len(errorlist[14])
#fileName = 'IMG_0236.jpg'

hStr = """
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
from matplotlib import ticker
%matplotlib inline
plt.figure(figsize=(8,5), dpi=80)
# 拿参数接收hist返回值，主要用于记录分组返回的值，标记数据标签
n, bins, patches = plt.hist(data13['carrier_no'], bins=11, rwidth=0.8, range=(1,12), align='left', label='xx直方图')
for i in range(len(n)):
    plt.text(bins[i], n[i]*1.02, int(n[i]), fontsize=12, horizontalalignment="center") #打标签，在合适的位置标注每个直方图上面样本数
plt.ylim(0,16000)
plt.title('直方图')
plt.legend()
# plt.savefig('直方图'+'.png')
plt.show()
"""

bStr = """#wooz['item_date'] = wooz['item_date'].astype('datetime')
    wooz['item_date'] = list(map(lambda q: datetime.datetime.strptime(q, '%Y-%m-%d'), wooz['item_date']))
    wooz['week_num'] = [str(idate.isocalendar()[0]) + 'W' + str(idate.isocalendar()[1]) for idate in wooz['item_date']]
    wooz = wooz.sort_values(by='item_date').reset_index(drop=True)
    for col in ['plays', 'downloads', 'item_duration', 'num_comment']:
        wooz[col] = wooz[col].astype('float')
    wooz['paid_item'] = wooz['paid_item'].astype(bool)
    wooz['item_id'] = wooz['item_id'].astype('str')
    wooz.to_excel(os.path.join(filePath, '北京话事人_888.xlsx'), sheet_name='北京话事人', index=False)"""

Astr = """
## Create some fake data.
    x1 = np.linspace(0.0, 5.0)
    y1 = np.cos(2 * np.pi * x1) * np.exp(-x1)
    x2 = np.linspace(0.0, 2.0)
    y2 = np.cos(2 * np.pi * x2)
    xdata = [i for i in range(1, 11)]

## use pyplot.subplot   method
    fig_1 = plt.figure(1, facecolor='y')
    ax_1 = plt.subplot(211)
    ax_1.plot(x1, y1)
    ax_2 = plt.subplot(212)
    ax_2.plot(x2, y2)
    ax_2.set_xlabel('time (s)')
    ax_2.set_ylabel('Undamped')
    plt.show()

## use pyplot.subplots  method
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1)
    # 制作一个三行一列的SUBplots SPace, 画布Fig, 和三个Subplots (ax1,2,3)
    fig, axs = plt.subplots(2, 2)
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2)
    # 制作一个 2*2 的SUBplots SPace, 画布Fig, 和矩阵【0，0】--> 【1，1】
    fig.suptitle('A tale of 2 subplots')
    ax1 = axs[0, 0]
    ax1.plot(x1, y1, 'o-', mfc='w', ms=3.5)
    ax1.set_ylabel('Damped oscillation')
    ax2 = axs[1, 1]
    ax2.plot(x2, y2, '.-')
    ax2.set_xlabel('time (s)')
    ax2.set_ylabel('Undamped')
    ax3 = axs[1, 0]
    ax3.plot(xdata,xdata, label='test')
    ax3.plot(xdata, [2*i for i in xdata], 'r--', label='test_2')
    ax3.set_facecolor('0.83')
    ax3.legend()
    ax2.grid(axis='x')
    ax4 = axs[0, 1]
    ax4.plot()
    plt.show()
"""



