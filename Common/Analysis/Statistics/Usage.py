from tqdm import tqdm
import logging
import json

def plug_in(data) :
    logger = logging.getLogger(__name__)
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        PROGRESS = SETTING['PROJECT']['PROGRESSBAR'].lower()
        DL = []

        if PROGRESS == 'true' :
            DATA_list = tqdm( range(len(data.computer_id)),
                                total=len(data.computer_id),
                                desc='Usage')
        else :
            DATA_list = range(len(data.computer_id))

        for c in DATA_list:
        # for c in range(len(data.computer_id)):
            if data.disk_used_space[c][0].isdigit() and data.disk_total_space[c][0].isdigit() :
                driveUsage = round(int(data.disk_used_space[c][0]) / int(data.disk_total_space[c][0]) * 100, 1)
            else :
                driveUsage = 'unconfirmed'

            if data.ram_use_size[c].isdigit() and data.ram_total_size[c].isdigit() :
                ramUsage = round(int(data.ram_use_size[c]) / int(data.ram_total_size[c]) * 100, 1)
            else :
                ramUsage = 'unconfirmed'

            if type(data.cup_consumption[c]) == float :
                cpuUsage = round(data.cup_consumption[c],1)
            elif type(data.cup_consumption[c]) == int :
                cpuUsage = data.cup_consumption[c]
            elif type(data.cup_consumption[c]) == str :
                cpuUsage = 'unconfirmed'
            else :
                cpuUsage = 'unconfirmed'


            # 사용량
            iscsi_used_space = []
            iscsiusage = []
            if data.iscsi_size[c][0].isdigit() and data.iscsi_free_space[c][0].isdigit():
                for i in range(len(data.iscsi_size[c])):
                    iscsi_size = int(data.iscsi_size[c][i])
                    iscsi_free_space = int(data.iscsi_free_space[c][i])
                    used_space_percentage = round((iscsi_size - iscsi_free_space), 1)
                    iscsi_used_space.append(used_space_percentage)

                    used_percentage = round((used_space_percentage / iscsi_size) * 100, 1)
                    iscsiusage.append(used_percentage)
            else:
                iscsiusage = 'unconfirmed'
                iscsi_used_space = 'unconfirmed'

            # iscsi_used_space = []
            # iscsiusage = []
            # def calculate_usage(iscsi_size, iscsi_free_space):
            #     used_space_percentage = round((iscsi_size - iscsi_free_space), 1)
            #     iscsi_used_space.append(used_space_percentage)
            #
            #     used_percentage = round((used_space_percentage / iscsi_size) * 100, 1)
            #     iscsiusage.append(used_percentage)
            #
            # if data.iscsi_size[c][0].isdigit() and data.iscsi_free_space[c][0].isdigit():
            #     for i in range(len(data.iscsi_size[c])):
            #         if len(data.iscsi_size[c]) > i and len(data.iscsi_free_space[c]) > i:
            #             iscsi_size = int(data.iscsi_size[c][i])
            #             iscsi_free_space = int(data.iscsi_free_space[c][i])
            #             calculate_usage(iscsi_size, iscsi_free_space)
            #         else:
            #             break
            # else:
            #     iscsi_used_space = 'unconfirmed'
            #     iscsiusage = 'unconfirmed'
            # iscsi_used_space = []
            # iscsiusage = []
            # if data.iscsi_size[c][0].isdigit() and data.iscsi_free_space[c][0].isdigit():
            #     iscsi_size = int(data.iscsi_size[c][0])
            #     iscsi_free_space = int(data.iscsi_free_space[c][0])
            #     used_space_percentage = round((iscsi_size - iscsi_free_space), 1)
            #     iscsi_used_space.append(used_space_percentage)
            #
            #     used_percentage = round((used_space_percentage / iscsi_size) * 100, 1)
            #     iscsiusage.append(used_percentage)
            #
            #     if len(data.iscsi_size[c]) > 1 and len(data.iscsi_free_space[c]) > 1:
            #         iscsi_size = int(data.iscsi_size[c][1])
            #         iscsi_free_space = int(data.iscsi_free_space[c][1])
            #         used_space_percentage = round((iscsi_size - iscsi_free_space), 1)
            #         iscsi_used_space.append(used_space_percentage)
            #
            #         used_percentage = round((used_space_percentage / iscsi_size) * 100, 1)
            #         iscsiusage.append(used_percentage)
            # else:
            #     iscsi_used_space.append('unconfirmed')
            #     iscsiusage.append('unconfirmed')
            # print(iscsi_used_space)




            # 리스트 형태 아닌것
            # if data.iscsi_size[c][0].isdigit() and data.iscsi_free_space[c][0].isdigit():
            #     iscsi_size = int(data.iscsi_size[c][0])
            #     iscsi_free_space = int(data.iscsi_free_space[c][0])
            #     used_space_percentage = round((iscsi_size - iscsi_free_space) , 1)
            #     iscsi_used_space = used_space_percentage
            #
            #     if len(data.iscsi_size[c]) > 1 and len(data.iscsi_free_space[c]) > 1:
            #         iscsi_size = int(data.iscsi_size[c][1])
            #         iscsi_free_space = int(data.iscsi_free_space[c][1])
            #         used_space_percentage = round((iscsi_size - iscsi_free_space), 1)
            #         iscsi_used_space = f"{iscsi_used_space}, {used_space_percentage}"
            # else:
            #     iscsi_used_space = 'unconfirmed'


            #원본
            # if data.iscsi_size[c][0].isdigit() and data.iscsi_free_space[c][0].isdigit() :
            #     iscsi_used_space = round(int(data.iscsi_size[c][0]) / int(data.iscsi_free_space[c][0]) * 100, 1)
            #     print(iscsi_used_space)
            # else :
            #     iscsi_used_space = 'unconfirmed'

            #사용률
            # iscsiusage = []
            # if data.iscsi_used_space[c][0].isdigit() and data.iscsi_size[c][0].isdigit():
            #     iscsi_used_space = int(data.iscsi_used_space[c][0])
            #     print(iscsi_used_space)
            #     iscsi_size = int(data.iscsi_size[c][0])
            #     used_percentage = round((iscsi_used_space / iscsi_size) * 100, 1)
            #     iscsiusage.append(used_percentage)
            #
            #     if len(data.iscsi_used_space[c]) > 1 and len(data.iscsi_size[c]) > 1:
            #         iscsi_used_space = int(data.iscsi_size[c][1])
            #         iscsi_size = int(data.iscsi_free_space[c][1])
            #         used_percentage = round((iscsi_used_space / iscsi_size) * 100, 1)
            #         iscsiusage.append(used_percentage)
            #         #print(iscsiusage)
            # else:
            #     iscsiusage='unconfirmed'
            #
            # print("-------------------------")
            # iscsiusage = []
            # if data.iscsi_size[c][0].isdigit() and data.iscsi_free_space[c][0].isdigit():
            #     iscsi_size = int(data.iscsi_size[c][0])
            #     iscsi_free_space = int(data.iscsi_free_space[c][0])
            #     used_percentage = round((iscsi_size / iscsi_free_space) * 100 , 1)
            #     iscsiusage.append(used_percentage)
            #
            #     if len(data.iscsi_size[c]) > 1 and len(data.iscsi_free_space[c]) > 1:
            #         iscsi_size = int(data.iscsi_size[c][1])
            #         iscsi_free_space = int(data.iscsi_free_space[c][1])
            #         used_percentage = round((iscsi_size / iscsi_free_space) * 100, 1)
            #         iscsiusage.append(used_percentage)
            # else:
            #     iscsiusage.append('unconfirmed')
                #iscsiusage = 'unconfirmed' 언컴펌드 리스트삭제


            #원본
            # if data.iscsi_size[c][0].isdigit() and data.iscsi_free_space[c][0].isdigit() :
            #     iscsiusage = round(int(data.iscsi_size[c][0]) / int(data.iscsi_free_space[c][0]) * 100, 1)
            # else :
            #     iscsiusage = 'unconfirmed'
            # print(iscsiusage)

            # iscsiusage = []
            # if data.iscsi_used_space[c][0].isdigit() and data.iscsi_size[c][0].isdigit():
            #     iscsi_used_space = int(data.iscsi_used_space[c][0])
            #     iscsi_size = int(data.iscsi_size[c][0])
            #     iscsiusage_percentage = round((iscsi_used_space / iscsi_size) * 100, 1)
            #     iscsiusage.append(iscsiusage_percentage)
            #
            #     if len(data.iscsi_used_space[c]) > 1 and len(data.iscsi_size[c]) > 1:
            #         iscsi_used_space = int(data.iscsi_used_space[c][1])
            #         iscsi_size = int(data.iscsi_size[c][1])
            #         iscsiusage_percentage = round((iscsi_used_space / iscsi_size) * 100, 1)
            #         iscsiusage.append(iscsiusage_percentage)
            #     else:
            #         iscsiusage.append('unconfirmed')
            # print(iscsiusage)

            DL.append([data.computer_id[c], str(driveUsage), str(ramUsage), str(cpuUsage),str(iscsi_used_space), str(iscsiusage)])
        logger.info('Usage.py - 성공')
        return DL
    except Exception as e:
        logger.warning('Usage.py - Error 발생')
        logger.warning('Error : {}'.format(str(e)))



    #'computer_id', 'last_reboot', 'disk_total_space', 'disk_used_space',
    #'os_platform', 'is_virtual', 'chassis_type',
    #'ipv_address', 'today_listen_port_count', 'yesterday_listen_port_count',
    #'today_established_port_count', 'yesterday_established_port_count',
    #'ram_use_size', 'ram_total_size', 'installed_applications_name',
    #'running_processes', 'cup_consumption'