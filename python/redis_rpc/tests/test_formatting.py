import glob
import os.path
import yapf

PACKAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..')


def test_formatting():
    files = glob.glob("%s/**/*.py" % PACKAGE_DIR, recursive=True)
    if yapf.main(['arg0-placeholder', '--diff'] + files) != 0:
        raise Exception("Please use yapf to format code before submitting.  "
                        "See the diff for details.")
