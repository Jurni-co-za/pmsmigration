import json
import shutil
import os


class PMS_Methods():

    def __init__(self):
        pass

    @staticmethod
    def read_json():
        f = open('config.json',)
        data = json.load(f)
        result = {}
        # print('path is {0}, username is {1}, password is {2},host is {3},database_name is {4} and port is {5}.'
        #       .format(data['img_path'],
        # data['db']['username'],
        # data['db']['password'],
        # data['db']['host'],
        # data['db']['database_name'],
        # data['db']['port']))
        # print(data)
        result['username'] = data['db']['username']
        result['password'] = data['db']['password']
        result['host'] = data['db']['host']
        result['database_name'] = data['db']['database_name']
        result['driver'] = data['db']['driver']

        f.close()
        return result

    def read_csv(self):
        pass
        # csv_file = "test_import.csv"
        # df = pd.read_csv(csv_file, header=0, sep=';')
        # json_list = json.loads(json.dumps(list(df.T.to_dict().values())))
        # for dic in json_list:
        #     print(dic)

    @staticmethod
    def copy_images():
        src_dir = "folder1"
        dst_dir = "folder2"
        file_location = '/srv/volume1/data/eds/eds_report.csv'
        file_name = os.path.basename(file_location)  # eds_report.csv
        location = os.path.dirname(file_location)  # /srv/volume1/data/eds

        fl = 'https://jurni.tech/web/image?model=property.pictures&id=598&field=image'
        fn = file_name = os.path.basename(fl)
        ln = os.path.dirname(fn)
        imageNames = ['en.png', 'qwe.pdf']
        for imageName in imageNames:
            shutil.copy(os.path.join(src_dir, imageName), dst_dir)
        print("copying files done")
