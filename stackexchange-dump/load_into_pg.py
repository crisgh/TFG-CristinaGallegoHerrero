#!/usr/bin/env python3
import sys
import time
import argparse
import os
import row_processor as Processor
import six
import json
import mysql.connector as pg
import xml.etree.ElementTree as ET
from collections import namedtuple
from pymysql.converters import escape_string
import pymysql

# requirements:
    # argparse==1.2.1
    # libarchive-c==2.9
    # lxml==4.6.3
    # six==1.10.0



# Special rules needed for certain tables (esp. for old database dumps)
specialRules = {("Posts", "ViewCount"): "NULLIF(%(ViewCount)s, '')::int"}

# part of the file already downloaded
file_part = None


def show_progress(block_num, block_size, total_size):
    """Display the total size of the file to download and the progress in percent"""
    global file_part
    if file_part is None:
        suffixes = ["B", "KB", "MB", "GB", "TB"]
        suffixIndex = 0
        pp_size = total_size
        while pp_size > 1024:
            suffixIndex += 1  # Increment the index of the suffix
            pp_size = pp_size / 1024.0  # Apply the division
        six.print_(
            "Total file size is: {0:.1f} {1}".format(pp_size, suffixes[suffixIndex])
        )
        six.print_("0 % of the file downloaded ...\r", end="", flush=True)
        file_part = 0

    downloaded = block_num * block_size
    if downloaded < total_size:
        percent = 100 * downloaded / total_size
        if percent - file_part > 1:
            file_part = percent
            six.print_(
                "{0} % of the file downloaded ...\r".format(int(percent)),
                end="",
                flush=True,
            )
    else:
        file_part = None
        six.print_("")

def getConnectionParameters():
    """Get the parameters for the connection to the database."""

    parameters = {}

    if args.dbname:
        parameters['dbname'] = args.dbname

    if args.host:
        parameters['host'] = "127.0.0.1"

    if args.port:
        parameters['port'] = "3306"

    if args.username:
        parameters['user'] = args.username

    if args.password:
        parameters['password'] = args.password

    if args.schema_name:
        parameters['options'] = "-c search_path=" + args.schema_name

    return parameters



def _createMogrificationTemplate(table, keys, insertJson):
    """Return the template string for mogrification for the given keys."""
    table_keys = ", ".join(
        [
            "%(" + k + ")s"
            if (table, k) not in specialRules
            else specialRules[table, k]
            for k in keys
        ]
    )
    if insertJson:
        return "(" + table_keys + ", %(jsonfield)s" + ")"
    else:
        return "(" + table_keys + ")"

def ins_query_maker(keys,cursor,tablename, rowdict,insertJson):
    """Use the cursor to mogrify a tuple of data.
    The passed data in `attribs` is augmented with default data (NULLs) and the
    order of data in the tuple is the same as in the list of `keys`. The
    `cursor` is used to mogrify the data and the `templ` is the template used
    for the mogrification.
    """
    keys = tuple(rowdict)
    dictsize = len(rowdict)
    sql = ''
    for i in range(dictsize) :
        if(type(rowdict[keys[i]]).__name__ == 'str'):
            sql += '\'' + str(rowdict[keys[i]].replace("'", "\\'")) + '\''
        else:
            sql += str(rowdict[keys[i]])
        if(i< dictsize-1):
            sql += ', '
    if insertJson:
        dict_attribs = {}
        for name, value in rowdict.items():
            dict_attribs[name] = value
        sql += ', \'' + str(escape_string(json.dumps(dict_attribs))) + '\''
        key = str(keys) + ', jsonfield'
        key = key.replace("'", "")
        key = key.replace(")", "")
        key = key + ")"
    else:
        key = str(keys)
        key = key.replace("'", "")

    query = "insert into " + str(tablename) + " " + key + " values (" + str(sql) + ");"
    return query

def _getTableKeys(table):
    """Return an array of the keys for a given table"""
    keys = None
    if table == "Users":
        keys = [
            "Id",
            "Reputation",
            "CreationDate",
            "DisplayName",
            "LastAccessDate",
            "WebsiteUrl",
            "Location",
            "AboutMe",
            "Views",
            "UpVotes",
            "DownVotes",
            "ProfileImageUrl",
            "Age",
            "AccountId",
        ]
    elif table == "Badges":
        keys = ["Id", "UserId", "Name", "Date", "Class","TagBased"]
    elif table == "PostLinks":
        keys = ["Id", "CreationDate", "PostId", "RelatedPostId", "LinkTypeId"]
    elif table == "Comments":
        keys = ["Id", "PostId", "Score", "Text", "CreationDate", "UserId","UserDisplayName","ContentLicense"]
    elif table == "Votes":
        keys = ["Id", "PostId", "VoteTypeId", "UserId", "CreationDate", "BountyAmount"]
    elif table == "Posts":
        keys = [
            "Id",
            "PostTypeId",
            "AcceptedAnswerId",
            "ParentId",
            "CreationDate",
            "Score",
            "ViewCount",
            "Body",
            "OwnerUserId",
            "OwnerDisplayName",
            "LastEditorUserId",
            "LastEditorDisplayName",
            "LastEditDate",
            "LastActivityDate",
            "Title",
            "Tags",
            "AnswerCount",
            "CommentCount",
            "ContentLicense",
            "FavoriteCount",
            "ClosedDate",
            "CommunityOwnedDate"
        ]
    elif table == "Tags":
        keys = ["Id", "TagName", "Count", "ExcerptPostId", "WikiPostId"]
    elif table == "PostHistory":
        keys = [
            "Id",
            "PostHistoryTypeId",
            "PostId",
            "RevisionGUID",
            "CreationDate",
            "UserId",
            "UserDisplayName",
            "Comment"
            "Text",
            "ContentLicense",
            "PostText"
        ]
    return keys


def handleTable(table, insertJson, createFk, mbDbFile):
    """Handle the table including the post/pre processing."""
    keys = _getTableKeys(table)
    dbFile = mbDbFile if mbDbFile is not None else table + ".xml"
    tmpl = _createMogrificationTemplate(table, keys, insertJson)
    start_time = time.time()

    try:
        pre = open("./sql/" + table + "_pre.sql").read()
        post = open("./sql/" + table + "_post.sql").read()
        fk = open("./sql/" + table + "_fk.sql").read()
    except IOError as e:
        six.print_(
            "Could not load pre/post/fk sql. Are you running from the correct path?",
            file=sys.stderr,
        )
        sys.exit(-1)

    try:
        #declare your database variables
        DBHOST = '127.0.0.1'
        DBNAME = args.dbname
        DBUSER = args.username
        DBPASS = args.password

        #establish the connection
        with pg.connect(host=DBHOST, database=DBNAME, user=DBUSER, passwd=DBPASS,charset='utf8', autocommit = True) as conn:

            # Get a cursor
            with conn.cursor(buffered=True) as cur:
                cur.execute("SET GLOBAL connect_timeout=31536000;")
                #conn.commit()
                #rows = cur.fetchall()
                try:
                    six.print_("mi create ...")
                    with open(dbFile, "rb") as xml:
                    # Pre-processing (dropping/creation of tables)
                        six.print_("Pre-processing ...")
                        if pre != "":
                            cur.execute(pre)
                            cur.close()
                            print("Database Updated !")
                        six.print_(
                            "Pre-processing took {:.1f} seconds".format(
                                time.time() - start_time
                            )
                        )

                        # Handle content of the table
                        start_time = time.time()
                        six.print_("Processing data ...")
                        for rows in Processor.batch(Processor.parse(xml), 500):
                            conn.reconnect()
                            cur = conn.cursor(buffered=True)
                            valuesStr = "".join(
                                [
                                    ins_query_maker(keys,cur,table, row_attribs,insertJson)
                                    for row_attribs in rows
                                ]

                            )
                            if len(valuesStr) > 0:
                                cur.execute(valuesStr)
                                cur.close()

                        six.print_(
                            "Table '{0}' processing took {1:.1f} seconds".format(
                                table, time.time() - start_time
                            )
                        )
                        conn.reconnect()
                        cur= conn.cursor(buffered=True)
                        if createFk:
                            # fk-processing (creation of foreign keys)
                            start_time = time.time()
                            six.print_("Foreign Key processing ...")
                            if post != "":
                                cur.execute(fk)
                                cur.close()

                            six.print_(
                                "Foreign Key processing took {0:.1f} seconds".format(
                                    time.time() - start_time
                                )
                            )
                        #conn.commit()
                        conn.close()
                except IOError as e:
                    six.print_(
                        "Could not read from file {}.".format(dbFile), file=sys.stderr
                    )
                    six.print_("IOError: {0}".format(e.strerror), file=sys.stderr)
    except pg.Error as e:
        six.print_("Error in dealing with the database.", file=sys.stderr)
        six.print_("Database Update Failed !: {}".format(e))

    except pg.Warning as w:
        six.print_("Warning from the database.", file=sys.stderr)
        six.print_("pg.Warning: {0}".format(str(w)), file=sys.stderr)

#############################################################

parser = argparse.ArgumentParser()
parser.add_argument(
    "-t",
    "--table",
    help="The table to work on.",
    choices=[
        "Users",
        "Badges",
        "Posts",
        "Tags",
        "Votes",
        "PostLinks",
        "PostHistory",
        "Comments",
    ],
    default=None,
)

parser.add_argument(
    "-d",
    "--dbname",
    help="Name of database to create the table in. The database must exist.",
    default="stackoverflow",
)

parser.add_argument(
    "-f", "--file", help="Name of the file to extract data from.", default=None
)

parser.add_argument(
    "-s", "--so-project", help="StackExchange project to load.", default=None
)

parser.add_argument(
    "--archive-url",
    help="URL of the archive directory to retrieve.",
    default="https://ia800107.us.archive.org/27/items/stackexchange",
)

parser.add_argument(
    "-k",
    "--keep-archive",
    help="Will preserve the downloaded archive instead of deleting it.",
    action="store_true",
    default=False,
)

parser.add_argument("-u", "--username", help="Username for the database.", default=None)

parser.add_argument("-p", "--password", help="Password for the database.", default=None)

parser.add_argument(
    "-P", "--port", help="Port to connect with the database on.", default=None
)

parser.add_argument("-H", "--host", help="Hostname for the database.", default=None)

parser.add_argument(
    "--with-post-body",
    help="Import the posts with the post body. Only used if importing Posts.xml",
    action="store_true",
    default=False,
)

parser.add_argument(
    "-j",
    "--insert-json",
    help="Insert raw data as JSON.",
    action="store_true",
    default=False,
)

parser.add_argument(
    "-n", "--schema-name", help="Use specific schema.", default="public"
)

parser.add_argument(
    "--foreign-keys", help="Create foreign keys.", action="store_true", default=False
)

args = parser.parse_args()

try:
    # Python 2/3 compatibility
    input = raw_input
except NameError:
    pass

# load given file in table
if args.file and args.table:
    table = args.table

    if table == "Posts":
        # If the user has not explicitly asked for loading the body, we replace it with NULL
        if not args.with_post_body:
            specialRules[("Posts", "Body")] = "NULL"

    choice = input("This will drop the {} table. Are you sure [y/n]?".format(table))

    if len(choice) > 0 and choice[0].lower() == "y":
        handleTable(
            table, args.insert_json, args.foreign_keys, args.file)
    else:
        six.print_("Cancelled.")

    exit(0)

# load a project
elif args.so_project:
    import py7zr
    import tempfile

    filepath = None
    temp_dir = None
    if args.file:
        filepath = args.file
        url = filepath
    else:
        # download the 7z archive in tempdir
        #file_name = args.so_project + ".stackexchange.com.7z"
        file_name = args.so_project + ".stackexchange.com.7z"
        url = "{0}/{1}".format(args.archive_url, file_name)
        temp_dir = tempfile.mkdtemp(prefix="so_")
        filepath = os.path.join(temp_dir, file_name)
        six.print_("Downloading the archive in {0}".format(filepath))
        six.print_("please be patient ...")
        try:
            six.moves.urllib.request.urlretrieve(url, filepath, show_progress)
        except Exception as e:
            six.print_(
                "Error: impossible to download the {0} archive ({1})".format(url, e)
            )
            exit(1)
    try:
        with py7zr.SevenZipFile(filepath) as z:
            z.extractall()
    except Exception as e:
        six.print_("Error: impossible to extract the {0} archive ({1})".format(url, e))
        exit(1)

    tables = [
        "Tags",
        "Users",
        "Badges",
        "Posts",
        "Comments",
        "Votes",
        "PostLinks",
        "PostHistory",
    ]

    for table in tables:
        six.print_("Load {0}.xml file".format(table))
        handleTable(table, args.insert_json, args.foreign_keys, None)
        # remove file
        os.remove(table + ".xml")

    if not args.keep_archive:
        os.remove(filepath)
        if temp_dir:
            # remove the archive and the temporary directory
            print("delete ",temp_dir)
            os.rmdir(temp_dir)
        else:
            six.print_("Archive '{0}' deleted".format(filepath))

    exit(0)

else:
    six.print_(
        "Error: you must either use '-f' and '-t'  arguments or the '-s' argument."
    )
    parser.print_help()
