import json
import argparse
import os
import re


def main():
    parser = argparse.ArgumentParser(description='create a new ptocore environment.')

    parser.add_argument('NAME', help='name of the new environment.')
    parser.add_argument('PATH', help='absolute path to where analyzer modules should be stored.')
    parser.add_argument('--config-file', default='-',
                        help='path to where the configuration should be saved. '
                             'use \'-\' for printing to standard output (default).')
    parser.add_argument('--mongo-file', default=None,
                        help='also create a MongoDB script and save it at the specified path. '
                             'use \'-\' for printing to standard output. you also need to specify --mongo-metadata.')

    parser.add_argument('--mongo-host', help='hostname or IP address of mongo server (default: localhost).', default='localhost')
    parser.add_argument('--mongo-port', type=int, help='port of mongo server (default 27017).', default=27017)
    parser.add_argument('--mongo-metadata', metavar='DATABASE',
                        help='if you want to create the MongoDB script, the name of the metadata database is needed.')

    parser.add_argument('--ask-passwords', action='store_true', default=False,
                        help='ask the user for the passwords instead of generating them.')

    parser.add_argument('--password-length', default=20, type=int,
                        help='set the length of generated passwords (default 20).')

    parser.add_argument('--supervisor-port', type=int, default=33424)
    parser.add_argument('--no-repo-cleaning', dest='ensure_clean_repo', action='store_false',
                        help='analyzer module repositories are reset and cleaned before executing. set this flag to '
                             'allow repositories to be dirty. useful in development environments because you don\'t '
                             'have to commit everytime to test an analyzer module.')

    args = parser.parse_args()

    if re.fullmatch("[a-zA-Z0-9]*", args.NAME) is None:
        print("The environment name '{}' contains illegal characters. Only the characters [a-zA-Z0-9] are allowed.".format(args.NAME))
        exit(-1)

    if args.config_file != '-' and os.path.exists(args.config_file):
        print("I will not overwrite the existing file '{}'.".format(args.config_file))
        exit(-1)

    if args.mongo_file is not None:
        if args.mongo_metadata is None:
            print("If you want to create the MongoDB script, please also specify --mongo-metadata.")
            exit(-1)
        if os.path.exists(args.mongo_file):
            print("I will not overwrite the existing file '{}'.".format(args.mongo_file))
            exit(-1)

    # prepare credentials
    password_characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234567890-_'

    def generate_password():
        return ''.join(password_characters[c % len(password_characters)] for c in os.urandom(args.password_length))

    name = args.NAME

    base_repo_path = args.PATH
    if os.path.isabs(base_repo_path) is False:
        print("Please specify an absolute path. '{}' is not absolute.".format(base_repo_path))
        exit(-1)

    sensor_username = name + '-sensor'
    sensor_password = input('sensor password: ') if args.ask_passwords else generate_password()

    supervisor_username = name + '-supervisor'
    supervisor_password = input('supervisor password: ') if args.ask_passwords else generate_password()

    validator_username = name + '-validator'
    validator_password = input('validator password: ') if args.ask_passwords else generate_password()

    admin_username = name + '-admin'
    admin_password = input('admin password: ') if args.ask_passwords else generate_password()

    # build configuration
    mongo_uri_format = 'mongodb://{u}:{p}@' + '{}:{}/'.format(args.mongo_host, args.mongo_port)

    doc = {
        'environment': name,
        'sensor': {
            'mongo_uri': mongo_uri_format.format(u=sensor_username, p=sensor_password)
        },
        'supervisor': {
            'mongo_uri': mongo_uri_format.format(u=supervisor_username, p=supervisor_password),
            'listen_port': args.supervisor_port,
            'ensure_clean_repo': args.ensure_clean_repo
        },
        'validator': {
            'mongo_uri': mongo_uri_format.format(u=validator_username, p=validator_password)
        },
        'admin': {
            'mongo_uri': mongo_uri_format.format(u=admin_username, p=admin_password),
            'base_repo_path': base_repo_path
        }
    }

    # store configuration
    if args.config_file == '-':
        print("\nConfiguration\n-------------")
        print(json.dumps(doc, indent=2))
    else:
        with open(args.config_file, 'wt') as fp:
            json.dump(doc, fp, indent=2)

    if args.mongo_file is not None:
        # gather the necessary arguments for the mongo script
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
                {'role': 'read',        'db': args.mongo_metadata},
                {'role': 'userAdmin',   'db': args.mongo_metadata},
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
                {'role': "readWrite", 'db': args.mongo_metadata},
                {'role': "readWrite", 'db': name+'-core'},
                {'role': "readWrite", 'db': name+'-temp'},
                {'role': "readWrite", 'db': name+'-obs'}
            ]
        }

        admin_create_user = {
            'user': admin_username,
            'pwd': admin_password,
            'roles': [
                {'role': 'read',        'db': args.mongo_metadata},
                {'role': 'readWrite',   'db': name+'-core'},
                {'role': 'readWrite',   'db': name+'-temp'},
                {'role': 'read',        'db': name+'-obs'}
            ]
        }

        # build script
        mongo_script =  "/*\n * MongoDB script file to create users for ptocore environment '{}'".format(name)
        mongo_script += " * Please make sure you are in the 'admin' database.\n */\n\n"
        mongo_script += "// sensor\n"
        mongo_script += "db.createUser(" + json.dumps(sensor_create_user, indent=2) + ");\n\n"
        mongo_script += "// supervisor\n"
        mongo_script += "db.createUser(" + json.dumps(supervisor_create_user, indent=2) + ");\n\n"
        mongo_script += "// validator\n"
        mongo_script += "db.createUser(" + json.dumps(validator_create_user, indent=2) + ");\n\n"
        mongo_script += "// admin\n"
        mongo_script += "db.createUser(" + json.dumps(admin_create_user, indent=2) + ");\n"

        if args.mongo_file == '-':
            print("\nMongoDB script\n--------------")
            print(mongo_script)
        else:
            with open(args.mongo_file, 'wt') as fp:
                fp.write(mongo_script)


if __name__ == "__main__":
    main()