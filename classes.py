import json


class SqlWithTransaction:
    def __init__(self, sqlCommand: str, transactionId: str) -> None:
        self.sqlCommand = sqlCommand
        self.transactionId = transactionId

    def toDict(self):
        return {"SqlCommand": self.sqlCommand, "TransactionId": self.transactionId}

    def __str__(self) -> str:
        return json.dumps(self.toDict())


class Operation:
    def __init__(self, name: str, parameters: list) -> None:
        self.name = name
        self.parameters = parameters

    def toDict(self):
        return {"Name": self.name, "Parameters": self.parameters}

    def __str__(self) -> str:
        return json.dumps(self.toDict())


class OperationWithTransaction:
    def __init__(self, operation: Operation, transactionId: str) -> None:
        self.operation = operation
        self.transactionId = transactionId

    def toDict(self):
        return {"Operation": self.operation.toDict(), "TransactionId": self.transactionId}

    def __str__(self) -> str:
        return json.dumps(self.toDict())


class TestResult:
    def __init__(self, result: str, sqlWithTransaction: SqlWithTransaction = None, operationWithTransaction: OperationWithTransaction = None) -> None:
        self.result = result
        self.sqlWithTransaction = sqlWithTransaction
        self.operationWithTransaction = operationWithTransaction

    def toDict(self):
        obj = {"TestRan": self.sqlWithTransaction.toDict(), "Result": self.result}
        if self.operationWithTransaction:
            obj["OperationRan"] = self.operationWithTransaction.toDict()
            # obj["OpeartionRan"] = self.operationWithTransaction.toDict()
        return obj

    def __str__(self) -> str:
        return json.dumps(self.toDict())
