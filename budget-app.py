#Create Flask Application
from functools import wraps
from flask import Flask, request, json
from flask_cors import CORS
import pyodbc 
import logging
import sys
app = Flask(__name__) 
CORS(app)

#Connect To Firebase
import firebase_admin
from firebase_admin import credentials, auth
cred = credentials.Certificate('fbAdminConfig.json')
firebase = firebase_admin.initialize_app(cred)

logger = logging.getLogger(__name__)

class MySQL:
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=')
        self.cursor = self.conn.cursor()

    def __exit__(self, *args):
        if self.conn:
            self.conn.close()
            self.conn = None

#Authentication Wrapper
def check_token(f):
    @wraps(f)
    def wrap(*args,**kwargs):
        if not request.headers.get('authorization'):
            logger.exception('Exception Occured during check_token: No token provided')
            return {'message': 'No token provided'}, 400
        try:
            user = auth.verify_id_token(request.headers['authorization'])
            if(request.json['userId'] != user['uid']):
                logger.exception('Exception Occured during check_token: Token provided does not match user Id provided.')
                return {'message':'Token provided does not match user Id provided.'},400
        except Exception as e:
            logger.exception('Exception Occured during check_token: No token provided')
            return {'message':'Invalid token provided.'},400
        return f(*args, **kwargs)
    return wrap

#Endpoints Below
@app.route("/") 
def index(): 
   return "Hello, World!" 

#__________________________AUTH_______________________________

@app.route("/SignUp", methods = ['POST']) 
@check_token
def SignUp(): 
    try:
        mysql = MySQL()
        with mysql:
            firstName = request.json['firstName'] 
            lastName = request.json['lastName'] 
            email = request.json['email']
            userId = request.json['userId']

            #Create User and Initial Categories in SQL
            addUserSQL =  "INSERT INTO BudgetDB.dbo.Users (UserId, FirstName, LastName, Email) VALUES " 
            addUserSQL += "('" + userId + "', '" + firstName + "', '" + lastName + "', '" + email + "')"     
            mysql.cursor.execute(addUserSQL)
            mysql.conn.commit()

            addCategoriesSql = "INSERT INTO BudgetDB.dbo.UserCategories (UserId, CategoryName, CategoryType, PlannedSpending) VALUES "
            addCategoriesSql += "('" + userId + "', 'Rent', 'Expense', 500),"
            addCategoriesSql += "('" + userId + "', 'Groceries', 'Expense', 300),"
            addCategoriesSql += "('" + userId + "', 'Social', 'Expense', 100),"
            addCategoriesSql += "('" + userId + "', 'Paycheck', 'Income', 1000)"
            mysql.cursor.execute(addCategoriesSql)
            mysql.conn.commit()

            result = {'message' : 'Success'}
            result['userObj'] = GetUserObj(userId)

            return app.response_class(response=json.dumps(result), mimetype='application/json')
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        logger.exception('Exception Occured at Signup(' + str(line_number) + ') - ' + str(e))
        return app.response_class(response=json.dumps({'error': str(e)}), mimetype='application/json'), 400

@app.route("/Login", methods = ['POST']) 
@check_token
def Login(): 
    try:
        userId = request.json['userId']
        result = {'message' : 'Success'}
        result['userObj'] = GetUserObj(userId)
        
        return app.response_class(response=json.dumps(result), mimetype='application/json')
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        logger.exception('Exception Occured at Login(' + str(line_number) + ') - ' + str(e))
        return app.response_class(response=json.dumps({'error': str(e)}), mimetype='application/json'), 400

def GetUserObj(userId):
    mysql = MySQL()
    with mysql:
        getUserSQL = "SELECT * FROM BudgetDB.dbo.Users Where UserId = '" + userId + "'"
        mysql.cursor.execute(getUserSQL)
        userResult = mysql.cursor.fetchone()

        user = {
            'userId' : userId,
            'firstName' : userResult[1],
            'lastName' : userResult[2],
            'email' : userResult[3]
        }

        getUserCategoriesSQL = "SELECT * FROM BudgetDB.dbo.UserCategories Where UserId='" + userId + "'"
        mysql.cursor.execute(getUserCategoriesSQL)
        userCategoriesResult = mysql.cursor.fetchall()
        expenseCategories = []
        incomeCategories = []
        if(userCategoriesResult is not None):
            for category in list(filter(lambda category: category[3] == "Expense", userCategoriesResult)):
                expenseCategories.append({'CategoryId' : category[0], 'CategoryName' : category[2], 'Planned' : float(category[4])})

            for category in list(filter(lambda category: category[3] == "Income", userCategoriesResult)):
                incomeCategories.append({'CategoryId' : category[0], 'CategoryName' : category[2], 'Planned' : float(category[4])})

        getTransactionsSQL = "SELECT T.TransactionId, T.UserCategoryId, T.Description, T.Amount, T.TransactionDate, T.TransactionType, T.UserId FROM dbo.Transactions T  Where T.UserId='" + userId + "' ORDER BY T.TransactionDate DESC, T.TransactionId DESC"
        mysql.cursor.execute(getTransactionsSQL)
        transactionsResult = mysql.cursor.fetchall()
        expenseTransactions = []
        incomeTransactions = []
        if(transactionsResult is not None):
            for transaction in transactionsResult:
                transDate = transaction[4].strftime("%m/%d/%Y")
                if transaction[5] == "Expense":
                    expenseTransactions.append({'TransactionId' : transaction[0], 'CategoryId' : transaction[1], 'Description' : transaction[2], 'Amount' : float(transaction[3]), 'Date' : transDate})
                else:
                    incomeTransactions.append({'TransactionId' : transaction[0], 'CategoryId' : transaction[1], 'Description' : transaction[2], 'Amount' : float(transaction[3]), 'Date' : transDate})
        return {
            'user' : user, 
            'expenseCategories' : expenseCategories, 
            'incomeCategories' : incomeCategories, 
            'expenseTransactions' : expenseTransactions, 
            'incomeTransactions' : incomeTransactions
        }

@app.route("/DeleteUser", methods = ['POST']) 
@check_token
def DeleteUser():
    try:
        mysql = MySQL()
        with mysql:
            userId = request.json['userId']

            deleteUserTransactionsSQL = "DELETE FROM BudgetDB.dbo.Transactions Where UserId = '" + userId + "'"
            deleteUserCategoriesSQL = "DELETE FROM BudgetDB.dbo.UserCategories Where UserId = '" + userId + "'"
            deleteUserSQL = "DELETE FROM BudgetDB.dbo.Users Where UserId = '" + userId + "'"
            
            mysql.cursor.execute(deleteUserTransactionsSQL)
            mysql.cursor.execute(deleteUserCategoriesSQL)
            mysql.cursor.execute(deleteUserSQL)
            mysql.conn.commit()

            result = {'message' : 'Success'}
            return app.response_class(response=json.dumps(result), mimetype='application/json')
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        logger.exception('Exception Occured at DeleteUser(' + str(line_number) + ') - ' + str(e))
        return app.response_class(response=json.dumps({'error': str(e)}), mimetype='application/json'), 400

#___________________TRANSACTIONS___________________________

@app.route("/AddTransaction", methods=['GET', 'POST']) 
@check_token
def AddTransaction():
    try:
        mysql = MySQL()
        with mysql:
            userId = request.json['userId']
            transactionType = request.json['transactionType']
            description = request.json['Description']
            categoryId = request.json['CategoryId']
            amount = request.json['Amount']
            date = request.json['Date']

            addTransactionSql = "INSERT INTO BudgetDB.dbo.Transactions (UserId, UserCategoryId, TransactionType, Description, Amount, TransactionDate) "
            addTransactionSql += "OUTPUT Inserted.TransactionId VALUES "
            addTransactionSql += "( '" + userId + "', " + str(categoryId) + ", '" + transactionType + "', '" + description.replace("'", "''") + "', " + str(amount) + ", '" + date + "')"
            mysql.cursor.execute(addTransactionSql)
            transactionId = mysql.cursor.fetchone()[0]
            mysql.conn.commit()
            
            return app.response_class(response=json.dumps({'message' : 'Success', 'transactionId' : transactionId}),mimetype='application/json')
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        logger.exception('Exception Occured at AddTransaction(' + str(line_number) + ') - ' + str(e))
        return app.response_class(response=json.dumps({'error': str(e)}), mimetype='application/json'), 400

@app.route("/UpdateTransaction", methods=['GET', 'POST']) 
@check_token
def UpdateTransaction():
    try:
        mysql = MySQL()
        with mysql:
            transaction = request.json['transaction']

            updateTransactionSQL = "UPDATE BudgetDB.dbo.Transactions SET "
            updateTransactionSQL += "UserCategoryId = " + str(transaction['CategoryId']) + ", "
            updateTransactionSQL += "Description = '" + transaction['Description'] + "', "
            updateTransactionSQL += "Amount = " + str(transaction['Amount']) + ", "
            updateTransactionSQL += "TransactionDate = '" + transaction['Date'] + "' "
            updateTransactionSQL += "WHERE TransactionId = " + str(transaction['TransactionId'])

            mysql.cursor.execute(updateTransactionSQL)
            mysql.conn.commit()

            response = app.response_class(
                response=json.dumps({'message' : 'Success'}),
                mimetype='application/json'
            )
            return response
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        logger.exception('Exception Occured at UpdateTransaction(' + str(line_number) + ') - ' + str(e))
        return app.response_class(response=json.dumps({'error': str(e)}), mimetype='application/json'), 400

@app.route("/DeleteTransaction", methods=['GET', 'POST']) 
def DeleteTransaction():
    try:
        mysql = MySQL()
        with mysql:
            transactionId = request.json['transactionId']

            deleteTransactionSQL =  "DELETE FROM BudgetDB.dbo.Transactions WHERE TransactionId = " +  str(transactionId)
            mysql.cursor.execute(deleteTransactionSQL)
            mysql.conn.commit()

            response = app.response_class(
                response=json.dumps({'message' : 'Success'}),
                mimetype='application/json'
            )
            return response
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        logger.exception('Exception Occured at DeleteTransaction(' + str(line_number) + ') - ' + str(e))
        return app.response_class(response=json.dumps({'error': str(e)}), mimetype='application/json'), 400

#_____________________CATEGORIES________________________

@app.route("/AddCategory", methods=['GET', 'POST']) 
@check_token
def AddCategory():
    try:
        mysql = MySQL()
        with mysql:
            userId = request.json['userId']
            category = request.json['category']
            categoryType = category['categoryType']

            addCategorySql = "INSERT INTO BudgetDB.dbo.UserCategories (UserId, CategoryName, CategoryType, PlannedSpending) "
            addCategorySql += "OUTPUT Inserted.UserCategoryId VALUES "
            addCategorySql += "( '" + userId + "', '" + str(category['CategoryName']) + "', '" + categoryType + "', " + str(category['Planned']) + ")"
            mysql.cursor.execute(addCategorySql)
            categoryId = mysql.cursor.fetchone()[0]
            mysql.conn.commit()

            response = app.response_class(
                response=json.dumps({'message' : 'Success', 'categoryId' : categoryId}),
                mimetype='application/json'
            )
            return response
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        logger.exception('Exception Occured at AddCategory(' + str(line_number) + ') - ' + str(e))
        return app.response_class(response=json.dumps({'error': str(e)}), mimetype='application/json'), 400

@app.route("/UpdateCategory", methods=['GET', 'POST']) 
@check_token
def UpdateCategory():
    try:
        mysql = MySQL()
        with mysql:
            category = request.json['category']

            updateCategorySQL = "UPDATE BudgetDB.dbo.UserCategories SET "
            updateCategorySQL += "CategoryName = '" + category['CategoryName'] + "', "
            updateCategorySQL += "PlannedSpending = " + str(category['Planned']) + " "
            updateCategorySQL += "WHERE UserCategoryId = " + str(category['CategoryId'])

            mysql.cursor.execute(updateCategorySQL)
            mysql.conn.commit()

            response = app.response_class(
                response=json.dumps({'message' : 'Success'}),
                mimetype='application/json'
            )
            return response
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        logger.exception('Exception Occured at UpdateCategory(' + str(line_number) + ') - ' + str(e))
        return app.response_class(response=json.dumps({'error': str(e)}), mimetype='application/json'), 400
    

@app.route("/DeleteCategory", methods=['GET', 'POST']) 
@check_token
def DeleteCategory():
    try:
        mysql = MySQL()
        with mysql:
            categoryId = request.json['categoryId']

            deleteCategorySQL =  "DELETE FROM BudgetDB.dbo.UserCategories WHERE UserCategoryId = " +  str(categoryId)
            mysql.cursor.execute(deleteCategorySQL)
            mysql.conn.commit()

            response = app.response_class(
                response=json.dumps({'message' : 'Success'}),
                mimetype='application/json'
            )
            return response
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        logger.exception('Exception Occured at DeleteCategory(' + str(line_number) + ') - ' + str(e))
        return app.response_class(response=json.dumps({'error': str(e)}), mimetype='application/json'), 400
    





















#_____________BELOW NOT USED_______________________________________________________________
@app.route("/DeleteTransactions", methods=['GET', 'POST']) 
def DeleteTransactions():
    transactionIdsToDelete = request.json['transactionIdsToDelete']

    inList = "("
    for transactionId in transactionIdsToDelete:
        inList += str(transactionId) + ','
    inList = inList[:-1]
    inList += ")"

    deleteTransactionssSQL =  "DELETE FROM BudgetDB.dbo.Transactions WHERE TransactionId IN " +  inList
    mysql.cursor.execute(deleteTransactionssSQL)
    mysql.conn.commit()

    response = app.response_class(
        response=json.dumps({'message' : 'Success'}),
        mimetype='application/json'
    )
    return response

@app.route("/AddCategories", methods=['GET', 'POST']) 
@check_token
def AddCategories():
    userId = request.json['userId']
    categoriesToAdd = request.json['categoriesToAdd']
    categoryType = request.json['categoryType']

    addCategoriesSql = "INSERT INTO BudgetDB.dbo.UserCategories (UserId, CategoryName, CategoryType, PlannedSpending) VALUES "
    for category in categoriesToAdd:
        addCategoriesSql += "( " + str(userId) + ", '" + str(category['CategoryName']) + "', '" + categoryType + "', " + str(category['PlannedSpending']) + "),"
    addCategoriesSql = addCategoriesSql[:-1]
    
    mysql.cursor.execute(addCategoriesSql)
    mysql.conn.commit()

    getCategoriesSQL =  "SELECT * FROM BudgetDB.dbo.UserCategories Where UserId="  + str(userId) + " and CategoryType='" + categoryType + "'" 
    mysql.cursor.execute(getCategoriesSQL)
    categoryResult = mysql.cursor.fetchall()
    categories = []
    for category in categoryResult:
        categories.append({'CategoryId' : category[0], 'UserId' : category[1], 'CategoryName' : category[2], 'Planned' : float(category[4])})

    response = app.response_class(
        response=json.dumps({'message' : 'Success', 'categories' : categories}),
        mimetype='application/json'
    )
    return response


@app.route("/AddTransactions", methods=['GET', 'POST']) 
def AddTransactions():
    userId = request.json['userId']
    transactionsToAdd = request.json['transactionsToAdd']
    transactionType = request.json['transactionType']

    addTransactionsSql = "INSERT INTO BudgetDB.dbo.Transactions (UserId, UserCategoryId, TransactionType, Description, Amount, TransactionDate) VALUES "
    for transaction in transactionsToAdd:
        addTransactionsSql += "( " + str(userId) + ", " + str(transaction['CategoryId']) + ", '" + transactionType + "', '" + transaction['Description'].replace("'", "''") + "', " + str(transaction['Amount']) + ", '" + transaction['Date'] + "'),"
    addTransactionsSql = addTransactionsSql[:-1]
    
    mysql.cursor.execute(addTransactionsSql)
    mysql.conn.commit()

    getTransactionsSQL = "SELECT T.TransactionId, T.UserCategoryId, T.Description, T.Amount, T.TransactionDate, T.TransactionType, T.UserId FROM dbo.Transactions T  Where T.UserId=" + str(userId) + " ORDER BY T.TransactionDate DESC, T.TransactionId DESC"
    mysql.cursor.execute(getTransactionsSQL)
    transactionsResult = mysql.cursor.fetchall()
    transactions = []
    for transaction in list(filter(lambda transaction: transaction[5] == transactionType, transactionsResult)):
        transDate = transaction[4].replace(hour=0, minute=0, second=0)
        transactions.append({'TransactionId' : transaction[0], 'CategoryId' : transaction[1], 'Description' : transaction[2], 'Amount' : float(transaction[3]), 'Date' : transaction[4]})

    response = app.response_class(
        response=json.dumps({'message' : 'Success', 'transactions' : transactions}),
        mimetype='application/json'
    )
    return response

@app.route("/DeleteCategories", methods=['GET', 'POST']) 
def DeleteCategories():
    categoryIdsToDelete = request.json['categoryIdsToDelete']

    inList = "("
    for categoryId in categoryIdsToDelete:
        inList += str(categoryId) + ','
    inList = inList[:-1]
    inList += ")"

    deleteCategoriesSQL =  "DELETE FROM BudgetDB.dbo.UserCategories WHERE UserCategoryId IN " +  inList
    mysql.cursor.execute(deleteCategoriesSQL)
    mysql.conn.commit()

    response = app.response_class(
        response=json.dumps({'message' : 'Success'}),
        mimetype='application/json'
    )
    return response

if __name__ == '__main__': 
   app.run(host='0.0.0.0',port="8000",debug=True)