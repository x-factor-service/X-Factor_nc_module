import math

import psycopg2
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from collections import defaultdict

import pandas as pd

with open("Common/Dashboard/setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())
DataLoadingType = SETTING['MODULE']['DataLoadingType']
DBHost = SETTING['DB']['DBHost']
DBPort = SETTING['DB']['DBPort']
DBName = SETTING['DB']['DBName']
DBUser = SETTING['DB']['DBUser']
DBPwd = SETTING['DB']['DBPwd']
AssetTNM = SETTING['DB']['AssetTNM']
StatisticsTNM = SETTING['DB']['StatisticsTNM']
BS = SETTING['FILE']
DBSelectTime = SETTING['DB']['DBSelectTime']
day = datetime.today().strftime("%Y-%m-%d")
RSU = SETTING['FILE']['RunningService_Except']['USE']


def plug_in(table, day, type):
    try:
        fiveMinutesAgo = (datetime.today() - timedelta(minutes=DBSelectTime)).strftime("%Y-%m-%d %H:%M:%S")
        fiveMinuteAgo = (datetime.today() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
        halfHourAgo = (datetime.today() - timedelta(minutes=35)).strftime("%Y-%m-%d %H:%M:%S")
        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        fiveDay = (datetime.today() - timedelta(5)).strftime("%Y-%m-%d")
        # monthDay = (datetime.today() - timedelta(30)).strftime("%Y-%m-%d")
        monthDay = (datetime.today() - relativedelta(days=31)).strftime("%Y-%m-%d")
        # ----------------------서버수량그래프 데이터 변경 추가 종윤 ----------------------
        lastYear = (datetime.today() - relativedelta(months=12)).strftime("%Y-%m-%d")
        lastDay = (datetime.today() - relativedelta(months=11)).strftime("%Y-%m-%d")
        lastMonth = pd.date_range(lastDay, periods=12, freq='M').strftime("%Y-%m-%d")
        Reportday2 = (datetime.today() - timedelta(2)).strftime("%Y-%m-%d")
        Reportday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")

        a = []
        for i in lastMonth:
            a.append(str(i))
        LM = tuple(a)

        # ------------------------------------------------------------------------------
        month_str = (datetime.today() - relativedelta(months=1)).strftime("%Y-%m-%d")
        SDL = []
        Conn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHost, DBPort, DBName, DBUser, DBPwd))
        Cur = Conn.cursor()
        if table == 'asset':
            if day == 'yesterday':
                query = """
                    select 
                        computer_id, disk_used_space, listen_port_count, established_port_count, asset_collection_date
                    from
                        """ + AssetTNM + """
                    where 
                        to_char(asset_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """'
                    order by computer_id desc
                """
            # if day == 'monthly':
            #     query = """
            #         select
            #             to_char(asset_collection_date , 'YYYY-MM-DD'),
            #             sum(case when is_virtual='Yes' then 1 else 0 end) as is_virtual,
            #             sum(case when is_virtual='No' then 1 else 0 end) as not_virtual
            #         from
            #              """ + AssetTNM + """
            #         where
            #             (chassis_type = 'Rack Mount Chassis' or chassis_type = 'Virtual' )
            #         and
            #             to_char(asset_collection_date , 'YYYY-MM-DD') > '""" + month_str + """'
            #         group by
            #             to_char(asset_collection_date , 'YYYY-MM-DD')
            #         order by
            #             to_char(asset_collection_date , 'YYYY-MM-DD')
            #         asc;
            #     """
            # if day == 'all':
            #     query ="""
            #         select
            #             *
            #         from
            #             """ + AssetTNM + """
            #     """
        if table == 'statistics':
            if day == 'yesterday':
                if type == '':
                    query = """ 
                        select 
                            classification, item, item_count, statistics_collection_date
                        from 
                            daily_statistics
                        where 
                            to_char(statistics_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """'
                        and 
                            NOT classification IN ('installed_applications')
                        and
                            NOT classification IN ('running_service')
                        and
                            NOT classification IN ('session_ip')
                        and 
                            classification NOT like '%group_%'
                        and
                            NOT item IN ('unconfirmed')
                    """

                if type == 'bannerNC':
                    query = """
                        select 
                            classification, item, item_count, statistics_collection_date
                        from
                            daily_statistics
                        where 
                            classification in ('online_asset', 'virtual', 'os', 'group_server_count')
                            and NOT item IN ('unconfirmed')
                            and to_char(statistics_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """'                  
                    """
            ######################################################### 페이징 시작 ###############################################################
            if day == 'osMore':
                query = """
                            select
                                item, item_count
                            from
                                minutely_statistics
                            where
                                classification = 'operating_system' 
                            and 
                                statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            and
                                (item Ilike '%""" + type[2] + """%' or
                                item_count Ilike '%""" + type[2] + """%')
                            order by item_count::INTEGER desc
                            LIMIT """ + type[0] + """
                            OFFSET (""" + type[1] + """-1) * """ + type[0] + """

                        """
            elif day == 'osCount':
                query = """
                        select
                            COUNT(*)
                        from
                            minutely_statistics
                        where
                            (item Ilike '%""" + type[2] + """%' or
                            item_count Ilike '%""" + type[2] + """%')
                        and 
                            classification = 'operating_system'
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                """
            elif day == 'serverBandByMore':
                query = """
                            select
                                item, item_count
                            from
                                minutely_statistics
                            where
                                classification ='group_server_count' 
                            and 
                                statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            and
                                (item Ilike '%""" + type[2] + """%' or
                                item_count Ilike '%""" + type[2] + """%')
                            order by item_count::INTEGER desc
                            LIMIT """ + type[0] + """
                            OFFSET (""" + type[1] + """-1) * """ + type[0] + """

                        """
            elif day == 'serverBandByCount':
                query = """
                        select
                            COUNT(*)
                        from
                            minutely_statistics
                        where
                            (item Ilike '%""" + type[2] + """%' or
                            item_count Ilike '%""" + type[2] + """%')
                        and 
                            classification ='group_server_count' 
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                """
            elif day == 'runningServiceMore':
                try:
                    runningservice_locate = SETTING['FILE']['RunningService_Except']['Location']
                    readXls = pd.read_excel(runningservice_locate)
                    running_remove = []
                    for i in readXls.index:
                        running_remove.append(readXls['Running Service'][i])
                        # print(i, readXls['Running Service'][i])
                    running_tu = tuple(running_remove)
                    running_remove = str(running_tu)
                    query = """
                                select
                                    item, item_count
                                from
                                    minutely_statistics
                                where
                                    classification = 'running_service'
                                and
                                    item NOT IN """ + running_remove + """
                                and 

                                    statistics_collection_date >= '""" + fiveMinutesAgo + """'
                                and
                                    (item Ilike '%""" + type[2] + """%' or
                                    item_count Ilike '%""" + type[2] + """%')
                                order by item_count::INTEGER desc
                                LIMIT """ + type[0] + """
                                OFFSET (""" + type[1] + """-1) * """ + type[0] + """
                            """
                except:
                    query = """
                                select
                                    item, item_count
                                from
                                    minutely_statistics
                                where
                                    classification = 'running_service'
                                and 
                                    statistics_collection_date >= '""" + fiveMinutesAgo + """'
                                and
                                    (item Ilike '%""" + type[2] + """%' or
                                    item_count Ilike '%""" + type[2] + """%')
                                order by item_count::INTEGER desc
                                LIMIT """ + type[0] + """
                                OFFSET (""" + type[1] + """-1) * """ + type[0] + """
                            """

            elif day == 'runningServiceCount':
                try:
                    runningservice_locate = SETTING['FILE']['RunningService_Except']['Location']
                    readXls = pd.read_excel(runningservice_locate)
                    running_remove = []
                    for i in readXls.index:
                        running_remove.append(readXls['Running Service'][i])
                        # print(i, readXls['Running Service'][i])
                    running_tu = tuple(running_remove)
                    running_remove = str(running_tu)
                    query = """
                                select
                                    COUNT(*)
                                from
                                    minutely_statistics
                                where
                                    item NOT IN """ + running_remove + """
                                and
                                    (item Ilike '%""" + type[2] + """%' or
                                    item_count Ilike '%""" + type[2] + """%')
                                and 
                                    classification = 'running_service'
                                and 
                                    statistics_collection_date >= '""" + fiveMinutesAgo + """'
                        """
                except:
                    query = """
                            select
                                COUNT(*)
                            from
                                minutely_statistics
                            where
                                (item Ilike '%""" + type[2] + """%' or
                                item_count Ilike '%""" + type[2] + """%')
                            and 
                                classification = 'running_service'
                            and 
                                statistics_collection_date >= '""" + fiveMinutesAgo + """'
                    """

            elif day == 'runningServiceMore2':
                query = """
                            select
                                item, item_count
                            from
                                minutely_statistics
                            where
                                classification = 'running_service'
                            and 
                                statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            and
                                (item Ilike '%""" + type[2] + """%' or
                                item_count Ilike '%""" + type[2] + """%')
                            order by item_count::INTEGER desc, item asc
                            LIMIT """ + type[0] + """
                            OFFSET (""" + type[1] + """-1) * """ + type[0] + """
                        """
            elif day == 'runningServiceCount2':
                query = """
                        select
                            COUNT(*)
                        from
                            minutely_statistics
                        where
                            (item Ilike '%""" + type[2] + """%' or
                            item_count Ilike '%""" + type[2] + """%')
                        and 
                            classification = 'running_service'
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                """
            # -------------------------------------------종윤-----------------------------------------
            elif day == 'connectDestinationIpMore':
                query = """
                        select
                            mssi.item, mssi.item_count,msl.computer_name
                        from 
                            (select * from minutely_statistics_session_ip mssi where
                            classification = 'session_ip' and statistics_collection_date >= '""" + fiveMinutesAgo + """' and item != 'NO' order by item_count::INTEGER desc limit 50) as mssi                             
                        left join 
                            minutely_statistics_list as msl                        
                        on 
                            split_part(mssi.item,':',1) = msl.ipv_address  
                        where     
                            classification = 'session_ip' and item != 'NO'
                        and
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                        and
                             (item Ilike '%""" + type[2] + """%' or             
                             item_count Ilike '%""" + type[2] + """%')
                        order by 
                            item_count::INTEGER desc
                        LIMIT """ + type[0] + """
                        OFFSET (""" + type[1] + """-1) * """ + type[0] + """




                        """

                # query = """
                #         select
                #             item, item_count, minutely_statistics_list.computer_name
                #         from
                #             minutely_statistics
                #         join minutely_statistics_list
                #         on split_part(minutely_statistics.item,':',1) = minutely_statistics_list.ipv_address
                #         where
                #             classification = 'session_ip' and item != 'NO'
                #         and
                #             statistics_collection_date >= '""" + fiveMinutesAgo + """'
                #         and
                #             (item Ilike '%""" + type[2] + """%' or
                #
                #             item_count Ilike '%""" + type[2] + """%')
                #         order by item_count::INTEGER desc
                #         LIMIT """ + type[0] + """
                #         OFFSET (""" + type[1] + """-1) * """ + type[0] + """
                #     """

            elif day == 'connectDestinationIpCount':
                query = """
                        select
                            Count(*)
                        from 
                            (select * from minutely_statistics_session_ip mssi where
                            classification = 'session_ip' and statistics_collection_date >= '""" + fiveMinutesAgo + """' and item != 'NO' order by item_count::INTEGER desc limit 50) as mssi 

                        left join 
                            minutely_statistics_list as msl                        
                        on 
                            split_part(mssi.item,':',1) = msl.ipv_address
                        where
                            (item Ilike '%""" + type[2] + """%' or
                            item_count Ilike '%""" + type[2] + """%')
                        and
                            classification = 'session_ip' and item != 'NO'
                        and
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'

                    """
                # query = """
                #         select
                #             Count(*)
                #         from
                #             minutely_statistics
                #
                #         join minutely_statistics_list
                #         on split_part(minutely_statistics.item,':',1) = minutely_statistics_list.ipv_address
                #         where
                #             (item Ilike '%""" + type[2] + """%' or
                #
                #             item_count Ilike '%""" + type[2] + """%')
                #         and
                #             classification = 'session_ip' and item != 'NO'
                #         and
                #             statistics_collection_date >= '""" + fiveMinutesAgo + """'
                #
                #     """
            elif day == 'connectSourceIpMore':

                query = """
                        select
                            ipv_address, computer_name, session_ip_count
                        from
                            minutely_statistics_list
                        where
                            asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                        and NOT ipv_address IN ('unconfirmed')

                        and
                             (ipv_address Ilike '%""" + type[2] + """%' or             
                             session_ip_count Ilike '%""" + type[2] + """%' or           
                             computer_name Ilike '%""" + type[2] + """%')
                        order by 
                            session_ip_count::INTEGER desc
                        LIMIT """ + type[0] + """
                        OFFSET (""" + type[1] + """-1) * """ + type[0] + """

                """

            elif day == 'connectSourceIpCount':
                query = """
                        select
                            Count(*)
                        from 
                            minutely_statistics_list                            
                        where
                            asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                        and NOT ipv_address IN ('unconfirmed')
                    """

            elif day == 'iscsiMore':
                query = """
                        select
                            ipv_address, computer_name, iscsi_name, iscsi_drive_letter, iscsi_size, iscsi_free_space, iscsi_used_space, iscsiusage
                        from
                            minutely_statistics_list
                        where
                            asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                        and 
                            NOT iscsi_name IN ('unconfirmed','no results')    
                        and
                            (ipv_address Ilike '%""" + type[2] + """%' or
                            computer_name Ilike '%""" + type[2] + """%' or
                            iscsi_name Ilike '%""" + type[2] + """%' or
                            iscsi_drive_letter Ilike '%""" + type[2] + """%' or
                            iscsi_size Ilike '%""" + type[2] + """%' or
                            iscsi_free_space Ilike '%""" + type[2] + """%' or
                            iscsi_used_space Ilike '%""" + type[2] + """%' or
                            iscsiusage Ilike '%""" + type[2] + """%')
                            order by iscsi_name desc
                        LIMIT """ + type[0] + """
                        OFFSET (""" + type[1] + """-1) * """ + type[0] + """
                    """
            elif day == 'iscsiCount':
                query = """
                        select
                            COUNT(*)
                         from
                            minutely_statistics_list 
                        where
                            (ipv_address Ilike '%""" + type[2] + """%' or
                            computer_name Ilike '%""" + type[2] + """%' or
                            iscsi_name Ilike '%""" + type[2] + """%' or
                            iscsi_drive_letter Ilike '%""" + type[2] + """%' or
                            iscsi_size Ilike '%""" + type[2] + """%' or
                            iscsi_free_space Ilike '%""" + type[2] + """%' or
                            iscsi_used_space Ilike '%""" + type[2] + """%' or
                            iscsiusage Ilike '%""" + type[2] + """%')
                        and 
                            NOT iscsi_name IN ('unconfirmed','no results')     
                        and
                            asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                    """

            # ---------------------------------------------------------------------------------------------------------

            elif day == 'physicalServerMore':
                query = """
                            select
                                item, item_count
                            from
                                minutely_statistics
                            where
                                classification = 'manufacturer'
                            and 
                                statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            and
                                (item Ilike '%""" + type[2] + """%' or
                                item_count Ilike '%""" + type[2] + """%')
                            order by item_count::INTEGER desc
                            LIMIT """ + type[0] + """
                            OFFSET (""" + type[1] + """-1) * """ + type[0] + """

                        """
            elif day == 'physicalServerCount':
                query = """
                        select
                            COUNT(*)
                        from
                            minutely_statistics
                        where
                            (item Ilike '%""" + type[2] + """%' or
                            item_count Ilike '%""" + type[2] + """%')
                        and 
                            classification = 'manufacturer'
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                """
            elif day == 'gpuServerMore':
                query = """
                            select
                                ipv_address, computer_name, nvidia_smi
                            from
                                minutely_statistics_list
                            where
                                NOT nvidia_smi IN ('unconfirmed', 'no results')
                            and 
                                asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            and
                                (ipv_address Ilike '%""" + type[2] + """%' or
                                computer_name Ilike '%""" + type[2] + """%' or
                                nvidia_smi Ilike '%""" + type[2] + """%' )
                                order by nvidia_smi desc
                            LIMIT """ + type[0] + """
                            OFFSET (""" + type[1] + """-1) * """ + type[0] + """
                        """
            elif day == 'gpuServerCount':
                query = """
                        select
                            COUNT(*)
                        from
                            minutely_statistics_list
                        where
                            (ipv_address Ilike '%""" + type[2] + """%' or
                            computer_name Ilike '%""" + type[2] + """%' or 
                            nvidia_smi Ilike '%""" + type[2] + """%')
                        and 
                            NOT nvidia_smi IN ('unconfirmed', 'no results')
                        and 
                            asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                """
            elif day == 'memoryMore':
                query = """
                            select
                                ipv_address, computer_name, ram_use_size, ram_total_size, ramusage
                            from
                                minutely_statistics_list
                            where

                                asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            and
                                (ipv_address Ilike '%""" + type[2] + """%' or
                                computer_name Ilike '%""" + type[2] + """%' or  
                                ram_use_size Ilike '%""" + type[2] + """%' or
                                ram_total_size Ilike '%""" + type[2] + """%' or
                                ramusage Ilike '%""" + type[2] + """%')
                            order by 
                                NULLIF(regexp_replace(ramusage, '[0-9]', '', 'g'), '') asc,
		                        NULLIF(regexp_replace(ramusage, '\D', '', 'g'), '')::int desc
                            LIMIT """ + type[0] + """
                            OFFSET (""" + type[1] + """-1) * """ + type[0] + """

                        """
            elif day == 'memoryCount':
                query = """
                        select
                            COUNT(*)
                        from
                            minutely_statistics_list
                        where
                            (ipv_address Ilike '%""" + type[2] + """%' or
                            computer_name Ilike '%""" + type[2] + """%' or  
                            ram_use_size Ilike '%""" + type[2] + """%' or
                            ram_total_size Ilike '%""" + type[2] + """%' or
                            ramusage Ilike '%""" + type[2] + """%')
                        and                            
                            asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                """
            elif day == 'cpuMore':
                query = """
                                select
                                    ipv_address, computer_name, cup_details_cup_speed, cpuusage
                                from
                                    minutely_statistics_list
                                where
                                    asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                                and
                                    (ipv_address Ilike '%""" + type[2] + """%' or
                                    computer_name Ilike '%""" + type[2] + """%' or
                                    cup_details_cup_speed Ilike '%""" + type[2] + """%' or
                                    cpuusage Ilike '%""" + type[2] + """%')
                                order by 
                                    NULLIF(regexp_replace(cpuusage, '[0-9]', '', 'g'), '') asc,
	                                NULLIF(regexp_replace(cpuusage, '\D', '', 'g'), '')::float desc
                                LIMIT """ + type[0] + """
                                OFFSET (""" + type[1] + """-1) * """ + type[0] + """

                            """
            elif day == 'cpuCount':
                query = """
                            select
                                COUNT(*)
                            from
                                minutely_statistics_list
                            where
                                    (ipv_address Ilike '%""" + type[2] + """%' or
                                    computer_name Ilike '%""" + type[2] + """%' or
                                    cup_details_cup_speed Ilike '%""" + type[2] + """%' or
                                    cpuusage Ilike '%""" + type[2] + """%')
                            and                               
                                asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                    """
            elif day == 'diskMore':
                query = """
                            select
                                ipv_address, computer_name, disk_used_space, disk_total_space, driveusage
                            from
                                minutely_statistics_list
                            where 
                                asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            and
                                (ipv_address Ilike '%""" + type[2] + """%' or
                                computer_name Ilike '%""" + type[2] + """%' or
                                disk_used_space Ilike '%""" + type[2] + """%' or
                                disk_total_space Ilike '%""" + type[2] + """%' or
                                driveusage Ilike '%""" + type[2] + """%')
                            order by 
                                NULLIF(regexp_replace(driveusage, '[0-9]', '', 'g'), '') asc,
	                            NULLIF(regexp_replace(driveusage, '\D', '', 'g'), '')::int desc
                            LIMIT """ + type[0] + """
                            OFFSET (""" + type[1] + """-1) * """ + type[0] + """

                        """
            elif day == 'diskCount':
                query = """
                        select
                            COUNT(*)
                        from
                            minutely_statistics_list
                        where
                                (ipv_address Ilike '%""" + type[2] + """%' or
                                computer_name Ilike '%""" + type[2] + """%' or
                                disk_used_space Ilike '%""" + type[2] + """%' or
                                disk_total_space Ilike '%""" + type[2] + """%' or
                                driveusage Ilike '%""" + type[2] + """%')
                        and 
                            asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                """
            elif day == 'alarmCaseMore':
                query = """
                        select 
                            ipv_address, computer_name, ram, cpu, drive, TF
                        from
                            minutely_statistics_list msl
                        inner join 
                        (select computer_id ,        
                        case when ramusage = 'unconfirmed' then -1 else ramusage::NUMERIC end as ram,
                        case when cpuusage = 'unconfirmed' then -1 else cpuusage::numeric end as cpu,
                        case when driveusage = 'unconfirmed' then -1 else driveusage::numeric end as drive,
                        case when asset_list_statistics_collection_date <= '""" + fiveMinutesAgo + """' and asset_list_statistics_collection_date >= '""" + halfHourAgo + """'then 'True' else 'False' end as TF
                        from
                            minutely_statistics_list) msli
                        on msl.computer_id = msli.computer_id
                        where 
                               (ram > 95 and asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                             or cpu > 95 and asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                             or drive > 99 and asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                             or asset_list_statistics_collection_date <= '""" + fiveMinutesAgo + """' and asset_list_statistics_collection_date >= '""" + halfHourAgo + """')
                        and
                            (ipv_address Ilike '%""" + type[2] + """%' or
                            computer_name Ilike '%""" + type[2] + """%' or                          
                            ramusage Ilike '%""" + type[2] + """%' or
                            cpuusage Ilike '%""" + type[2] + """%' or
                            driveusage Ilike '%""" + type[2] + """%' or
                            TF Ilike '%""" + type[2] + """%')
                        LIMIT """ + type[0] + """
                        OFFSET (""" + type[1] + """-1) * """ + type[0] + """
                """
            elif day == 'alarmCaseCount':
                query = """
                        select 
                            COUNT(*)
                        from
                            minutely_statistics_list msl
                        inner join
                        (select computer_id,
                        case when ramusage = 'unconfirmed' then -1 else ramusage::NUMERIC end as ram,
                        case when cpuusage = 'unconfirmed' then -1 else cpuusage::numeric end as cpu,
                        case when driveusage = 'unconfirmed' then -1 else driveusage::numeric end as drive,
                        case when asset_list_statistics_collection_date < '""" + halfHourAgo + """' then 'True' else 'False' end as TF
                        from
                            minutely_statistics_list) msli
                        on msl.computer_id = msli.computer_id
                        where 
                             (ram > 95 and asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                             or cpu > 95 and asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                             or drive > 99 and asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                             or asset_list_statistics_collection_date <= '""" + fiveMinutesAgo + """' and asset_list_statistics_collection_date >= '""" + halfHourAgo + """')
                        and
                            (ipv_address Ilike '%""" + type[2] + """%' or
                            computer_name Ilike '%""" + type[2] + """%' or                          
                            ramusage Ilike '%""" + type[2] + """%' or
                            cpuusage Ilike '%""" + type[2] + """%' or
                            driveusage Ilike '%""" + type[2] + """%' or
                            TF Ilike '%""" + type[2] + """%')
                """
            #################################################################### 페이징 끝 ########################################################################
            if day == 'today':
                if type == '':
                    query = """ 
                        select 
                            classification, item, item_count, statistics_collection_date
                        from 
                            minutely_statistics
                        where 
                            NOT classification IN ('installed_applications')
                        and
                            NOT classification IN ('running_processes')
                        and
                            NOT classification IN ('session_ip')
                        and 
                            classification NOT like '%group_%'
                        and
                            NOT item IN ('unconfirmed')
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                    """

                elif type == 'bar':
                    query = """
                        select 
                            item, item_count 
                        from 
                            minutely_statistics  
                        where 
                            classification ='asset'
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                        order by 
                            item_count desc limit 3    
                    """
                elif type == 'pie':
                    query = """
                        select item, item_count from 
                        minutely_statistics 
                        where 
                            classification = 'os'
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """' 
                        order by item_count::INTEGER desc limit 3
                    """
                elif type == 'os_version':
                    query = """
                        select 
                            item, item_count
                        from 
                            minutely_statistics
                        where 
                            classification = 'operating_system' 
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                        order by item_count::INTEGER desc limit 8
                    """
                elif type == 'donut':
                    query = """
                        select 
                            item, item_count 
                        from 
                            minutely_statistics
                        where
                            classification = 'installed_applications'
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                        order by
                            item_count::INTEGER 
                        desc limit 5
                    """

                elif type == 'case':
                    query = """
                        select
                            computer_id, ipv_address, driveusage
                        from
                            minutely_statistics_list
                    """

                # NC 대역별 barchart
                elif type == 'group_server_count':
                    query = """
                            select 
                                item, item_count 
                            from 
                                minutely_statistics  
                            where 
                                classification ='group_server_count'
                            and 
                                statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            order by
                                item_count::INTEGER 
                            desc limit 3
                        """
                # NC running service chart
                elif type == 'running':
                    # 러닝서비스 프로그램 지우기
                    try:
                        runningservice_locate = SETTING['FILE']['RunningService_Except']['Location']
                        readXls = pd.read_excel(runningservice_locate, na_values='None')
                        running_remove = []
                        for i in readXls.index:
                            running_remove.append(readXls['Running Service'][i])
                            # print(i, readXls['Running Service'][i])
                        running_tu = tuple(running_remove)
                        running_remove = str(running_tu)
                        query = """
                            select
                                item, item_count
                            from
                                minutely_statistics
                            where 
                                classification = 'running_service'
                            and
                                item NOT IN """ + running_remove + """
                            and 
                                statistics_collection_date >= '""" + fiveMinutesAgo + """'

                            order by
                                item_count::INTEGER desc limit 5
                        """
                    except:
                        query = """
                                select
                                    item, item_count
                                from
                                    minutely_statistics
                                where 
                                    classification = 'running_service'
                                and 
                                    statistics_collection_date >= '""" + fiveMinutesAgo + """'

                                order by
                                    item_count::INTEGER desc limit 5
                            """
                # 알람케이스
                elif type == 'usage':
                    query = """
                        select
                            classification, item, item_count
                        from
                            minutely_statistics
                        where 
                            classification in ('ram_usage_size_exceeded', 'cpu_usage_size_exceeded', 'drive_usage_size_exceeded', 'last_online_time_exceeded')
                        and
                            NOT item IN ('unconfirmed', 'No', 'Safety')
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'

                    """
                # 물리서버 벤더별 수량
                elif type == 'vendor':
                    query = """
                        select
                            item, item_count
                        from
                            minutely_statistics
                        where
                            classification = 'manufacturer'
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                        order by
                            item_count::INTEGER desc 
                        limit 3
                    """
                # IP 대역별 총 알람 수(Top5)
                elif type == 'group_alarm':
                    query = """
                            select
                                item, item_count
                            from
                                minutely_statistics
                            where
                                classification IN
                                ('group_ram_usage_exceeded',
                                'group_last_online_time_exceeded',
                                'group_cpu_usage_exceeded',
                                'group_drive_usage_size_exceeded')
                                and statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            """
                elif type == 'bannerNC':
                    query = """
                            select 
                                classification, item, item_count, statistics_collection_date
                            from
                                minutely_statistics
                            where 
                                classification in ('online_asset', 'virtual', 'os', 'group_server_count')
                                AND item != 'unconfirmed'
                                and statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            """
                elif type == 'gpu':
                    query = """
                            select
                                item, item_count
                            from
                                minutely_statistics
                            where
                                classification = 'nvidia_smi'
                            and
                                item = 'YES'
                            and 
                                statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            union all
                            select
                                item, item_count
                            from
                                daily_statistics
                            where
                                classification = 'nvidia_smi'
                            and
                                item = 'YES'
                            and 
                                to_char(statistics_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """'
                    """

                elif type == 'iscsi':
                    query = """
                            select
                                item, item_count
                            from
                                minutely_statistics
                            where
                                classification = 'iscsi_name'
                            and
                                item = 'YES'
                            and 
                                statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            union all
                            select
                                item, item_count
                            from
                                daily_statistics
                            where
                                classification = 'iscsi_name'
                            and
                                item = 'YES'
                            and 
                                to_char(statistics_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """'
                    """
                # elif type == 'ip':
                #     query = """
                #             select
                #                 item, item_count
                #             from
                #                 minutely_statistics
                #             where
                #                 classification = 'session_ip' and item != 'NO'
                #             and
                #                 statistics_collection_date >= '""" + fiveMinutesAgo + """'
                #             order by
                #                 item_count::INTEGER desc limit 3
                #     """

                # -------------------------------------------종윤-----------------------------------------
                elif type == 'ip':
                    query = """
                            select
                                minutely_statistics_unique, classification, item, item_count
                            from
                                minutely_statistics
                            where
                                classification = 'session_ip_computer_name'
                            and
                                statistics_collection_date >= '""" + fiveMinuteAgo + """'
                            order by
                                item_count::INTEGER desc 
                    """
            # NC 서버 총 수량 추이 그래프(30일)
            if day == 'monthly':
                if type == 'asset':
                    # -----------------------------수정 종윤------------------------

                    query = """
                                select
                                    item,
                                    item_count,
                                    statistics_collection_date
                                from
                                    daily_statistics
                                where
                                    to_char(statistics_collection_date, 'YYYY-MM-DD')
                                in 
                                    """ + str(LM) + """
                                and
                                    classification = 'virtual'
                                and
                                    item != 'unconfirmed'
                                union
                                select
                                    item,
                                    item_count,
                                    statistics_collection_date
                                from
                                    minutely_statistics
                                where
                                    classification = 'virtual'
                                and
                                    item != 'unconfirmed'
                                and
                                    statistics_collection_date >= '""" + fiveMinutesAgo + """'
                                order by
                                    statistics_collection_date ASC;
                            """
                    # query = """
                    #             select
                    #                 item,
                    #                 item_count,
                    #                 statistics_collection_date
                    #             from
                    #                 daily_statistics
                    #             where
                    #                 to_char(statistics_collection_date, 'YYYY-MM-DD') > '""" + monthDay + """'
                    #             and
                    #                 classification = 'virtual'
                    #             and
                    #                 item != 'unconfirmed'
                    #             union
                    #             select
                    #                 item,
                    #                 item_count,
                    #                 statistics_collection_date
                    #             from
                    #                 minutely_statistics
                    #             where
                    #                 classification = 'virtual'
                    #             and
                    #                 item != 'unconfirmed'
                    #             and
                    #                 statistics_collection_date >= '""" + fiveMinutesAgo + """'
                    #             order by
                    #                 statistics_collection_date ASC;
                    #         """

            if day == 'fiveDay':
                if type == 'asset':
                    query = """ 
                        select 
                            classification,
                            item, 
                            item_count, 
                            statistics_collection_date
                        from 
                            daily_statistics 
                        where 
                            to_char(statistics_collection_date, 'YYYY-MM-DD') > '""" + fiveDay + """' 
                        and
                            classification = '""" + type + """'
                        order by
                            item_count desc;
                    """
            if day == 'assetItem':
                if type == 'Group':
                    query = """
                        select 
                            item, 
                            item_count  
                        from 
                            minutely_statistics 
                        where 
                            classification ='asset' 
                        and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                    """
            if type == 'ram':
                query = """
                    select
                        classification, item, item_count
                    from
                        minutely_statistics
                    where 
                        classification in ('group_ram_usage_exceeded')
                    and 
                            statistics_collection_date >= '""" + fiveMinutesAgo + """'
                    order by
                        item_count::INTEGER desc limit 5
                """
            if type == 'cpu':
                query = """
                    select
                        classification, item, item_count
                    from
                        minutely_statistics
                    where 
                        classification in ('group_cpu_usage_exceeded')
                    and 
                        statistics_collection_date >= '""" + fiveMinutesAgo + """'
                    order by
                        item_count::INTEGER desc limit 5
                """
            if type == 'world':
                query = """
                    select
                        classification, item
                    from
                        minutely_statistics
                    where 
                        classification in ('group_cpu_usage_exceeded', 'group_ram_usage_exceeded', 'group_running_service_count_exceeded', 
                        'group_last_reboot', 'drive_usage_size_exceeded')
                    and 
                        statistics_collection_date >= '""" + fiveMinutesAgo + """'
                """
        if table == 'statistics_list':
            if day == 'today':
                if type == 'DUS':
                    query = """
                            select
                                computer_id, driveusage, ipv_address
                            from
                                minutely_statistics_list
                        """
                elif type == 'statistics':
                    query = """
                        select
                            classification, item, item_count
                        from
                            minutely_statistics
                        where 
                            classification IN ('established_port_count_change', 
                                'group_running_service_count_exceeded',
                                'group_cpu_usage_exceeded',
                                'group_ram_usage_exceeded',
                                'listen_port_count_change',
                                'group_last_reboot',
                                'drive_usage_size_exceeded')
                            and 
                                NOT item IN ('unconfirmed')

                    """
                elif type == 'LH':
                    query = """
                        select
                            computer_id, last_reboot, ipv_address
                        from
                            minutely_statistics_list
                    """
                elif type == 'RUS':
                    query = """
                        select
                            computer_id, ramusage, ipv_address
                        from
                            minutely_statistics_list
                    """
                elif type == 'server':
                    query = """
                        select
                            ipv_address, computer_name, session_ip_count
                        from
                            minutely_statistics_list
                        where
                            asset_list_statistics_collection_date >= '""" + fiveMinutesAgo + """'
                            and ipv_address != 'unconfirmed'
                        order by
                            session_ip_count::INTEGER desc limit 3
                    """
                elif type == 'memoryMore':
                    query = """
                        select
                            ipv_address, computer_name, ram_use_size, ram_total_size, ramusage
                        from
                            minutely_statistics_list
                        order by
                            ramusage desc
                    """
            if day == 'yesterday':
                if type == 'DUS':
                    query = """
                        select
                            computer_id, driveusage, ipv_address
                        from
                            daily_statistics_list
                        where 
                            to_char(asset_list_statistics_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """' 
                    """
                elif type == 'LH':
                    query = """
                        select
                            computer_id, last_reboot, ipv_address
                        from
                            daily_statistics_list
                        where 
                            to_char(asset_list_statistics_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """' 
                    """
        Cur.execute(query)
        RS = Cur.fetchall()
        global_index = 0
        for i, R in enumerate(RS, start=1):
            if day == 'memoryMore' or day == 'diskMore':
                index = (int(type[1]) - 1) * int(type[0]) + i
                SDL.append(dict(
                    (
                        ('index', index),
                        ('ip', R[0]),
                        ('name', R[1]),
                        ('use', R[2]),
                        ('total', R[3]),
                        ('usage', str(R[4]))
                    )
                ))
            elif day == 'cpuMore':
                index = (int(type[1]) - 1) * int(type[0]) + i
                SDL.append(dict(
                    (
                        ('index', index),
                        ('ip', R[0]),
                        ('name', R[1]),
                        ('use', R[2]),
                        ('usage', str(R[3]))
                    )
                ))
            elif day == 'osMore' or day == 'serverBandByMore' or day == 'runningServiceMore' or day == 'runningServiceMore2' or day == 'physicalServerMore':
                index = (int(type[1]) - 1) * int(type[0]) + i
                SDL.append(dict(
                    (
                        ('index', index),
                        ('name', R[0]),
                        ('count', R[1]),
                    )
                ))
            elif day == 'gpuServerMore':
                index = (int(type[1]) - 1) * int(type[0]) + i
                SDL.append(dict(
                    (
                        ('index', index),
                        ('ip', R[0]),
                        ('name', R[1]),
                        ('model', R[2]),
                    )
                ))
            elif day == 'alarmCaseMore':
                index = (int(type[1]) - 1) * int(type[0]) + i
                SDL.append(dict(
                    (
                        ('index', index),
                        ('ip', R[0]),
                        ('name', R[1]),
                        ('ramusage', R[2]),
                        ('cpuusage', R[3]),
                        ('driveusage', R[4]),
                        ('date', R[5]),

                    )
                ))

            elif day == 'connectDestinationIpMore':

                index = (int(type[1]) - 1) * int(type[0]) + i

                if R[2] != None:
                    name = R[2]
                else:
                    name = 'undefined'
                SDL.append(dict(
                    (
                        ('index', index),
                        ('ip', R[0]),
                        ('count', R[1]),
                        ('name', name),

                    )
                ))

            elif day == 'connectSourceIpMore':

                index = (int(type[1]) - 1) * int(type[0]) + i
                SDL.append(dict(
                    (
                        ('index', index),
                        ('ip', R[0]),
                        ('name', R[1]),
                        ('count', R[2]),

                    )
                ))

            # elif day == 'iscsiMore':
            #     # index = (int(type[1]) - 1) * int(type[0]) + i
            #     iscsi_names = R[2].strip('{"}').replace('"', '').split(',')
            #     iscsi_drive_letter = R[3].replace('"', '').strip('{"}').split(',')
            #     iscsi_sizes = R[4].strip('{"}').split(',')
            #     iscsi_free_spaces = R[5].strip('{"}').split(',')
            #     iscsi_used_spaces = R[6].strip('["]').split(',')
            #     iscsiusages = R[7].strip('["]').split(',')
            #
            #     for iscsi_name,iscsi_drive_letter, iscsi_size,iscsi_free_space, iscsi_used_space, iscsiusage in zip(iscsi_names,iscsi_drive_letter,iscsi_sizes, iscsi_free_spaces,iscsi_used_spaces,iscsiusages):
            #         global_index += 1
            #         SDL.append(dict(
            #             (
            #                 ('index', global_index),
            #                 ('ip', R[0]),
            #                 ('name', R[1]),
            #                 ('iscsi_name', iscsi_name + ' (' + iscsi_drive_letter+')'),
            #                 ('iscsi_size', iscsi_size),
            #                 ('iscsi_free_space', iscsi_free_space),
            #                 ('iscsi_used_space', iscsi_used_space),
            #                 ('iscsiusage', iscsiusage),
            #             )
            #         ))

            elif day == 'iscsiMore':
                iscsi_names = R[2].strip('{"}').replace('"', '').split(',')
                iscsi_drive_letter = R[3].replace('"', '').strip('{"}').split(',')
                iscsi_sizes = R[4].strip('{"}').split(',')
                iscsi_free_spaces = R[5].strip('{"}').split(',')
                iscsi_used_spaces = R[6].strip('["]').split(',')
                iscsiusages = R[7].strip('["]').split(',')

                iscsi_name_text = '<br>'.join([iscsi_name + ' (' + iscsi_drive_letter + ')' for iscsi_name, iscsi_drive_letter in zip(iscsi_names, iscsi_drive_letter)])
                iscsi_size_text = '<br>'.join(iscsi_sizes)
                iscsi_free_space_text = '<br>'.join(iscsi_free_spaces)
                iscsi_used_space_text = '<br>'.join(iscsi_used_spaces)
                iscsiusage_text = '<br>'.join(iscsiusages)

                global_index += 1
                SDL.append(dict(
                    (
                        ('index', global_index),
                        ('ip', R[0]),
                        ('name', R[1]),
                        ('iscsi_name', iscsi_name_text),
                        ('iscsi_size', iscsi_size_text),
                        ('iscsi_free_space', iscsi_free_space_text),
                        ('iscsi_used_space', iscsi_used_space_text),
                        ('iscsiusage', iscsiusage_text),
                    )
                ))
            else:
                SDL.append(R)
        return SDL
    except:
        print(table + str(type) + day + ' Daily Table connection(Select) Failure')
    return