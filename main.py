import mariadb
import mariadb.constants.CLIENT as CLIENT
import os
import json
from classes import *


def parseTestCases(path: str) -> tuple[list[list[SqlWithTransaction]], list[list[OperationWithTransaction]]]:
    with open(path, "r") as f:
        obj = json.load(f)

        sqlInterleavingList = [[SqlWithTransaction(
            b["SqlCommand"], b["TransactionId"]) for b in a] for a in obj["SqlInterleavings"]]

        operationInterleavingList = []
        for a in obj["Interleaving"]:
            tmp = []
            for b in a:
                op = b["Operation"]
                operation = Operation(op["Name"], op["Parameters"])
                tmp.append(OperationWithTransaction(
                    operation, b["TransactionId"]))
            operationInterleavingList.append(tmp)

        return sqlInterleavingList, operationInterleavingList


def ExecuteCommandAsync(conn: mariadb.Connection, sql: SqlWithTransaction, op: OperationWithTransaction) -> TestResult:
    sql.sqlCommand = sql.sqlCommand.replace(
        "main", "main").replace("second_table", "second_table")
    result = TestResult("", sql, op)
    try:
        cur = conn.cursor()
        cur.execute(sql.sqlCommand)
        if "SELECT" in sql.sqlCommand.upper():
            for tup in cur:
                result.result += f"{' '.join([str(val) for val in tup])}\n"
        else:
            result.result = "Success"
    except Exception as e:
        result.result = f"Fail\n {e.args[0]}"
    return result


def ExecuteSingleTestCaseAsync(count: dict[str, int], sqlInterleavings: list[SqlWithTransaction], operationInterleavings: list[OperationWithTransaction]):
    assert len(sqlInterleavings) == len(operationInterleavings)
    history: list[TestResult] = []
    transactions = set()
    connections: dict[str, mariadb.Connection] = {}
    for tid in count:
        conn = mariadb.connect(
            user="root", password="123", host="localhost", port=9999)
        cur = conn.cursor()
        cur.execute("USE testDB;")
        conn.autocommit = False
        connections[tid] = conn
    for i in range(len(sqlInterleavings)):
        sql = sqlInterleavings[i]
        op = operationInterleavings[i]
        tid = sql.transactionId
        conn = connections[tid]
        if tid not in transactions:
            conn.begin()
            transactions.add(tid)
            history.append(TestResult(
                "Success", SqlWithTransaction("Begin", tid)))
        try:
            history.append(ExecuteCommandAsync(conn, sql, op))
        except Exception as e:
            history.append(TestResult(e.args[0], sql, op))
        count[tid] -= 1
        if count[tid] <= 0:
            try:
                conn.commit()
                history.append(TestResult(
                    "Success", SqlWithTransaction("Commit", tid)))
            except e:
                history.append(TestResult(
                    f"Fail \n {e.args[0]}", SqlWithTransaction("Commit", tid)))
    for conn in connections.values():
        conn.close()
    return history


def ExecuteTestAsync(path: str):
    sqlInterleavingList, operationInterleavingList = parseTestCases(path)
    assert len(sqlInterleavingList) == len(operationInterleavingList)
    histories = []
    for i in range(len(sqlInterleavingList)):
        assert len(sqlInterleavingList[i]) == len(operationInterleavingList[i])
        count = {}
        for j in range(len(operationInterleavingList[i])):
            assert sqlInterleavingList[i][j].transactionId == operationInterleavingList[i][j].transactionId
            tid = operationInterleavingList[i][j].transactionId
            if tid not in count:
                count[tid] = 0
            count[tid] += 1
        history = ExecuteSingleTestCaseAsync(
            count, sqlInterleavingList[i], operationInterleavingList[i])
        histories.append(history)
    return histories


def setup():
    isoLevel = "SERIALIZABLE"
    try:
        conn = mariadb.connect(user="root", password="123",
                               host="localhost", port=9999, client_flag=CLIENT.MULTI_STATEMENTS)
        cur = conn.cursor()
        cur.execute(f"SET GLOBAL TRANSACTION ISOLATION LEVEL {isoLevel}")
        cur.execute("DROP DATABASE IF EXISTS testDB;")
        cur.execute("CREATE DATABASE testDB;")
        cur.execute("USE testDB;")
        cur.execute("DROP TABLE IF EXISTS main;\
                    DROP TABLE IF EXISTS second_table;\
                    CREATE TABLE main(`key` varchar(99),\
                        value_store varchar(99),\
                        second_key varchar(99),\
                        PRIMARY KEY (`key`));\
                    CREATE TABLE second_table(`key` varchar(99),\
                        value_store varchar(99),\
                        second_key varchar(99),\
                        PRIMARY KEY (`key`));")
        conn.close()
        if not os.path.exists("results"):
            os.mkdir("results")
    except mariadb.Error as e:
        print(e.args[0])


def cleanup():
    try:
        conn = mariadb.connect(user="root", password="123",
                               host="localhost", port=9999, client_flag=CLIENT.MULTI_STATEMENTS)
        cur = conn.cursor()
        cur.execute("USE testDB;")
        cur.execute("TRUNCATE TABLE main; TRUNCATE TABLE second_table;")
        conn.close()
    except mariadb.Error as e:
        print(e.args[0])


def main():
    setup()
    for it in os.scandir("testcases"):
        res = ExecuteTestAsync(it.path)
        res = [[j.toDict() for j in i] for i in res]
        with open(f"results/{it.name}", "w") as f:
            f.write(json.dumps(res))
    cleanup()


if __name__ == "__main__":
    main()
