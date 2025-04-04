from flask import Flask, jsonify, request
from typing import Dict, Optional
import psycopg2
import json
import sys


app = Flask(__name__)


class Repository:  # выбор репозитория
    def __init__(self, file_path):
        self.file_path = file_path
        self.param_connection = self.__init_repository()

    def __init_repository(self):
        try:
            with open(self.file_path, 'r') as file:
                load_config = json.load(file)
                repository = load_config['repository']
                if repository == 'postgres':#если значение postgres, то будет загружена postgre, иначе работа в памяти
                    params_database = {
                        'dbname': load_config['dbname'],
                        'user': load_config['user'],
                        'password': load_config['password'],
                        'host': load_config['host'],
                        'port': load_config['port']
                    }
                else:
                    params_database = None
                return params_database
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            print(f"error file load: {e}")
            sys.exit(1)


config_path = 'config.json'
data_to_connect_db = Repository(config_path).param_connection


class User:
    def __init__(self, user_id: int, username: str):
        self.id = user_id
        self.username = username

    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'username': self.username
        }


class Proc_manage:
    def __init__(self, data_conn):
        self.__conn_param = data_conn
        if not data_conn:
            self.__users = {}
            self.__current_id = 1

    def proc_to_create_user(self, user_name: str):#proc_название - обработчики соответствующей  функции
        if self.__conn_param:#если параметры не пустые, то работа в postgre
            try:
                result = self.__execute_db_operation(
                    'INSERT INTO "user" (user_name) VALUES (%s) RETURNING id;',
                    (user_name,),
                    'create'
                )
                return json.dumps(result, ensure_ascii=False)
            except Exception as e:
                return json.dumps({'error': str(e)})
        else:#работа в памяти
            response = self.__create_user(user_name)
            return json.dumps(response.to_dict(), ensure_ascii=False)

    def proc_to_update_user(self, user_name: str, id: int):
        if self.__conn_param:#обновление данных (postgre)
            try:
                result = self.__execute_db_operation(
                    'UPDATE "user" SET user_name = %s WHERE id = %s;',
                    (user_name, id),
                    'update'
                )
                return json.dumps(result, ensure_ascii=False)
            except Exception as e:
                return json.dumps({'error': str(e)})
        else:
            response = self.__update_user(id, user_name)
            if not response:
                response = jsonify({"error": "user not found"})
                return response
            response = json.dumps(response.to_dict(), ensure_ascii=False)
            return response

    def proc_to_get_user(self, id: int):
        if self.__conn_param:#получение данных user в user(postgre)
            try:
                result = self.__execute_db_operation(
                    'SELECT * FROM "user" WHERE id = %s;',
                    (id,),
                    'get'
                )
                if not result:
                    return json.dumps({'error': 'user not found'})
                return json.dumps(result, ensure_ascii=False)
            except Exception as e:
                return json.dumps({'error': str(e)})
        else:
            response = self.__get_user(id)
            if response is None:
                response = jsonify({'error': 'user not found'})
                return response
            response = json.dumps(response.to_dict(), ensure_ascii=False)
            return response

    def proc_delete_user(self, id: int):
        if self.__conn_param:#удаление user по id в postgre
            try:
                result = self.__execute_db_operation(
                    'DELETE FROM "user" WHERE id = %s;',
                    (id,),
                    'delete'
                )
                return jsonify(result)
            except Exception as e:
                return jsonify({'error': str(e)})
        else:
            response = self.__delete_user(id)
            if response:
                return jsonify({'message': 'user deleted'})
            return jsonify({'error': 'user not found'})

    def __create_user(self, user_name: str) -> User:#запись в память нового user
        user = User(self.__current_id, user_name)
        self.__users[user.id] = user
        self.__current_id += 1
        return user

    def __update_user(self, id: int, user_name: str) -> Optional[User]:#изменение данных user
        if id not in self.__users:
            return None
        self.__users[id].username = user_name
        return self.__users[id]

    def __get_user(self, id: int) -> Optional[User]:#получение user по id
        return self.__users.get(id)

    def __delete_user(self, id: int) -> bool:#удаление user по id
        if id in self.__users:
            del self.__users[id]
            return True
        return False

    def __connect_func(self, params_database):#инициализация подключения к бд
        try:
            connect = psycopg2.connect(**params_database)
        except psycopg2.Error as e:
            print(f"connection error: {e}")
            sys.exit(1)
        return connect

    def __execute_db_operation(#ф-ция работы с postgresql(вынес дублирование в отдельную ф-цию)
        self,
        sql: str,
        params: tuple = None,
        operation_type: str = None
    ):
        connect = self.__connect_func(self.__conn_param)
        cursor = connect.cursor()
        try:
            cursor.execute(sql, params or ())

            if operation_type == 'create':
                id = cursor.fetchone()[0]
                connect.commit()
                return {"id": id, "user_name": params[0]} if params else None

            elif operation_type == 'update':
                connect.commit()
                if cursor.rowcount > 0:
                    return {"id": params[1], "user_name": params[0]}
                else:
                    return {'error': 'user not found'}

            elif operation_type == 'get':
                result = cursor.fetchone()
                connect.commit()
                return (
                    {"id": result[0], "user_name": result[1]}
                    if result else None
                )

            elif operation_type == 'delete':
                connect.commit()
                if cursor.rowcount > 0:
                    return {'message': 'user deleted'}
                else:
                    return {'error': 'user not found'}

        except Exception as e:
            connect.rollback()
            raise e
        finally:
            cursor.close()
            connect.close()

    def __check_exists(self):
        print("")

manage = Proc_manage(data_to_connect_db)


@app.route('/create_user', methods=['POST'])
def create_user():
    if not request.json or 'user_name' not in request.json:
        return jsonify({'error': 'no user_name'})
    try:
        response = manage.proc_to_create_user(request.json['user_name'])
        return response
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/update_user/<int:user_id>', methods=['PUT'])
def update_user(user_id):
    if not request.json or 'user_name' not in request.json:
        return jsonify({'error': 'no user_name'})
    response = manage.proc_to_update_user(request.json['user_name'], user_id)
    return response


@app.route('/get_user_by_id/<int:user_id>', methods=['GET'])
def get_user_by_id(user_id):
    user_get = manage.proc_to_get_user(user_id)
    return user_get


@app.route('/delete_user/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    user_delete = manage.proc_delete_user(user_id)
    return user_delete


if __name__ == '__main__':
    app.run(debug=True)
















