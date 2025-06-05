def classFactory(iface):
    import os
    import sys

    # import_path = os.path.join('/home/gereon/git/dima/benchi/venv/lib/python3.11/site-packages')
    # for f in ["/usr/local/lib/python3.11/dist-packages", "/usr/lib/python3/dist-packages", "/usr/lib/python3.11/dist-packages"]:
    #     sys.path.insert(0, os.path.join(f))
    # import_path = os.path.join('/usr/local/lib/python3.11/dist-packages')
    # sys.path.insert(0, import_path)

    # print("test")

    from hub.ravengui import Raven

    return Raven(iface)
