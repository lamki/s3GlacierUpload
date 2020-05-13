import boto3
import sys, os
import sqlite3
from sqlite3 import Error


def getFilename(date):
    # print('uag-'+date+'.[loop number]'+'.10.sql.gz')
    fileList = []
    num = 0
    for i in range(0, 6):
        filename = 'uag-'+date+ '.{:0>2}'.format(num) + '.10.sql.gz'
        fileList.append(filename)
        num += 4
    return fileList


def upload2(client, body, fn):
    response = client.upload_archive(
        accountId='-',
        archiveDescription=fn,
        body=body,
        vaultName='WC_vault',
    )
    return response


def storeDetail(fileName, responseMetadata, location, checksum, archiveId):
    conn = None
    try:
        conn = sqlite3.connect(r"glacier.db")
        # print(sqlite3.version)

        cur = conn.cursor()

        # FETCH last ID
        cur.execute("SELECT MAX(ID) FROM glacierBackup")
        data = cur.fetchall()
        lastId = data[0][0]

        # INSERT DATA
        id = lastId + 1

        query = "INSERT INTO glacierBackup (id, filename, rspnMetadata, location, checksum, archiveId) VALUES (?, ?, ?, ?, ?, ?)"
        count = cur.execute(query, (str(id), str(fileName), str(responseMetadata), location, checksum, archiveId))
        conn.commit()
        cur.close()
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def delFile(date):
    fileNameArr = getFilename(date)

    for i in range(0, len(fileNameArr)):
        os.system('rm -f '+fileNameArr[i])


# Main function
def main(date):
    fileDate = date[1]
    if len(date) < 2:
        print("Date is not defined. Exiting...")
        exit()
    else:
        list = getFilename(fileDate)

        for fn in list:
            print(fn)

            client = boto3.client('glacier')

            response = ''
            size = os.stat(fn).st_size

            with open(fn, 'rb') as upload:
                archive_upload = upload.read(size)
                response = upload2(client, archive_upload, fn)

            responseMetadata = response['ResponseMetadata']
            location = response['location']
            checksum = response['checksum']
            archiveId = response['archiveId']

            storeDetail(fn, responseMetadata, location, checksum, archiveId)

        delFile(fileDate)


# Call for main function
if __name__ == "__main__":
    client = boto3.client('glacier')
    main(sys.argv)
