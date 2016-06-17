import argparse

from ptocore.admin import app
from ptocore.coreconfig import CoreConfig

def main():
    desc = 'Manage analyzers and monitor system status with this web interface.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('config_file', type=argparse.FileType('rt'))
    args = parser.parse_args()

    cc = CoreConfig('admin', args.config_file)

    app.run(host=cc.admin_host, debug=True)

if __name__ == '__main__':
    main()