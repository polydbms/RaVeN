def classFactory(iface):
    import os
    import sys

    import_path = os.path.join('/home/gereon/git/dima/benchi/venv/lib/python3.11/site-packages')
    sys.path.insert(0, import_path)

    # print("test")

    from hub.ravengui import Raven

    return Raven(iface)
