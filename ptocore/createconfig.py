import json
import argparse
import os
import re


def main():
    parser = argparse.ArgumentParser(description='Create a new ptocore environment.'
                                                 'Also prints the necessary mongo commands to stdout.')
    parser.add_argument('NAME', help='Name of the new environment.')
    parser.add_argument('OUTPUT_DIR', help='Path to directory in where the config file should be stored.')
    parser.add_argument('--metadata', metavar='DATABASE.COLLECTION', required=True,
                        help='Specify database and collection where the metadata resides.'
                        'If this is omitted, the program will ask for it interactively.')
    parser.add_argument('--ptoadmin-path', help='Path to the pto-admin directory.', required=True)


    parser.add_argument('--ask-passwords', action='store_true', default=False,
                        help='Ask the user for the passwords instead of creating them using urandom.')
    parser.add_argument('--password-length', default=20, type=int,
                        help='Define the length of generated passwords.')
    parser.add_argument('--supervisor-port', type=int, default=33424)
    parser.add_argument('--admin-host', default='localhost')
    parser.add_argument('--admin-port', type=int, default=5000)

    args = parser.parse_args()
    if not os.path.isdir(args.OUTPUT_DIR):
        print("The given path '{}' is not a directory.".format(args.OUTPUT_DIR))
        exit(-1)

    if re.fullmatch("[a-zA-Z0-9]*", args.NAME) is None:
        print("The environment name '{}' contains illegal characters. Only the characters [a-zA-Z0-9] are allowed.".format(args.NAME))
        exit(-1)

    output_fn = os.path.join(args.OUTPUT_DIR, args.NAME+'.json')
    if os.path.exists(output_fn):
        print("I will not overwrite the existing file '{}'.".format(output_fn))
        exit(-1)

    # prepare credentials
    password_characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234567890-_'

    def generate_password():
        return ''.join(password_characters[c % len(password_characters)] for c in os.urandom(args.password_length))

    name = args.NAME

    metadata_db_name, metadata_coll_name = args.metadata.split('.', 2)

    sensor_username = name + '-sensor'
    sensor_password = input('sensor password: ') if args.ask_passwords else generate_password()

    supervisor_username = name + '-supervisor'
    supervisor_password = input('supervisor password: ') if args.ask_passwords else generate_password()

    validator_username = name + '-validator'
    validator_password = input('validator password: ') if args.ask_passwords else generate_password()

    admin_username = name + '-admin'
    admin_password = input('admin password: ') if args.ask_passwords else generate_password()

    # build configuration
    mongo_uri_format = 'mongodb://{u}:{p}@localhost/'

    doc = {
        'environment': name,
        'sensor': {
            'mongo_uri': mongo_uri_format.format(u=sensor_username, p=sensor_password)
        },
        'supervisor': {
            'mongo_uri': mongo_uri_format.format(u=supervisor_username, p=supervisor_password),
            'listen_port': args.supervisor_port
        },
        'validator': {
            'mongo_uri': mongo_uri_format.format(u=validator_username, p=validator_password)
        },
        'admin': {
            'mongo_uri': mongo_uri_format.format(u=admin_username, p=admin_password),
            'static_path': 'path/to/pto-admin',
            'listen_host': args.admin_host,
            'listen_port': args.admin_port
        },

        'metadata_coll': [metadata_db_name, metadata_coll_name]
    }

    # store configuration
    with open(output_fn, 'wt') as fp:
        json.dump(doc, fp, indent=2)

    # build commands for creating the users in mongodb
    sensor_create_user = {
        'user': sensor_username,
        'pwd': sensor_password,
        'roles': [
            {'role': 'readWrite', 'db': name+'-core'}
        ]
    }

    supervisor_create_user = {
        'user': supervisor_username,
        'pwd': supervisor_password,
        'roles': [
            {'role': 'read',        'db': metadata_db_name},
            {'role': 'userAdmin',   'db': metadata_db_name},
            {'role': 'dbOwner',     'db': name+'-core'},
            {'role': 'dbOwner',     'db': name+'-temp'},
            {'role': 'read',        'db': name+'-obs'},
            {'role': 'userAdmin',   'db': name+'-obs'}
        ]
    }

    validator_create_user = {
        'user': validator_username,
        'pwd': validator_password,
        'roles': [
            {'role': "readWrite", 'db': metadata_db_name},
            {'role': "readWrite", 'db': name+'-core'},
            {'role': "readWrite", 'db': name+'-temp'},
            {'role': "readWrite", 'db': name+'-obs'}
        ]
    }

    admin_create_user = {
        'user': admin_username,
        'pwd': admin_password,
        'roles': [
            {'role': 'read',        'db': metadata_db_name},
            {'role': 'readWrite',   'db': name+'-core'},
            {'role': 'readWrite',   'db': name+'-temp'},
            {'role': 'read',        'db': name+'-obs'}
        ]
    }

    # and present it to the user
    print("use admin;")
    print("// sensor")
    print("db.createUser(" + json.dumps(sensor_create_user, indent=2) + ");\n")
    print("// supervisor")
    print("db.createUser(" + json.dumps(supervisor_create_user, indent=2) + ");\n")
    print("// validator")
    print("db.createUser(" + json.dumps(validator_create_user, indent=2) + ");\n")
    print("// admin")
    print("db.createUser(" + json.dumps(admin_create_user, indent=2) + ");\n")


if __name__ == "__main__":
    main()