Total_Tenants=$(insync_rds -e "SELECT COUNT( customer.id ) AS Total_Tenants FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND customer_license.type = 'Commercial' AND customer_license.total_m365_users > 0;" 2>> /dev/null)
ap1Total_Tenants=$(insync_rds_prod_D1 -e "SELECT COUNT( customer.id ) AS Total_Tenants FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND customer_license.type = 'Commercial' AND customer_license.total_m365_users > 0;" 2>> /dev/null)
Total_Tenants1=`echo $Total_Tenants| awk -F" " '{print $2}'`
ap1Total_Tenants1=`echo $ap1Total_Tenants| awk -F" " '{print $2}'`
sumTotal_Tenants1=`expr $Total_Tenants1 + $ap1Total_Tenants1`
echo -e "\nPut Total_Tenants = $sumTotal_Tenants1 - in B4,B5,B6,B7,B8,B9 cells of newly created document\n"
###############################

Total_Lic_Users=$(insync_rds -e "SELECT SUM( customer_license.total_m365_users ) AS Total_Lic_Users FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND customer_license.type = 'Commercial' AND customer_license.total_m365_users > 0;" 2>> /dev/null)
ap1Total_Lic_Users=$(insync_rds_prod_D1 -e "SELECT SUM( customer_license.total_m365_users ) AS Total_Lic_Users FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND customer_license.type = 'Commercial' AND customer_license.total_m365_users > 0;" 2>> /dev/null)
Total_Lic_Users1=`echo $Total_Lic_Users| awk -F" " '{print $2}'`
ap1Total_Lic_Users1=`echo $ap1Total_Lic_Users| awk -F" " '{print $2}'`
sumTotal_Lic_Users1=`expr $Total_Lic_Users1 + $ap1Total_Lic_Users1`
echo -e "Put Total_Lic_Users = $sumTotal_Lic_Users1 - in C4,C5,C6,C7,C8,C9 cells of newly created document\n"
######################################################################

Active_Users=$(insync_rds -e "SELECT COUNT(product_ca_consumption.id) AS Active_Users FROM customer, customer_license, product_ca_consumption WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND product_ca_consumption.customerid = customer.id AND customer_license.type = 'Commercial' AND product_ca_consumption.m365_license_state in (1);" 2>> /dev/null)
ap1Active_Users=$(insync_rds_prod_D1 -e "SELECT COUNT(product_ca_consumption.id) AS Active_Users FROM customer, customer_license, product_ca_consumption WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND product_ca_consumption.customerid = customer.id AND customer_license.type = 'Commercial' AND product_ca_consumption.m365_license_state in (1);" 2>> /dev/null)
Active_Users1=`echo $Active_Users| awk -F" " '{print $2}'`
ap1Active_Users1=`echo $ap1Active_Users| awk -F" " '{print $2}'`
sumActive_Users1=`expr $Active_Users1 + $ap1Active_Users1`
echo -e "Put Total_Active_Users =  $sumActive_Users1 - in D4,D5,D6,D7,D8,D9 cells of newly created document\n"
######################################################################

Active_DevicesFS=$(insync_rds -e "SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM ( SELECT COUNT( IF( device.device_disabled = 0, 1, NULL ) ) AS Active_Devices, SUM(device.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM customer, customer_license, device, usertable WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND usertable.customerid = customer.id AND device.customerid = customer_license.customerid AND device.cloudapp_backup_initiated = True AND device.user_id = usertable.id AND customer_license.type = 'Commercial' AND device.cplatform = 'Office 365 Exchange Online' GROUP BY customer_license.customerid ) X\G;" 2>> /dev/null)
ap1Active_DevicesFS=$(insync_rds_prod_D1 -e "SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM ( SELECT COUNT( IF( device.device_disabled = 0, 1, NULL ) ) AS Active_Devices, SUM(device.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM customer, customer_license, device, usertable WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND usertable.customerid = customer.id AND device.customerid = customer_license.customerid AND device.cloudapp_backup_initiated = True AND device.user_id = usertable.id AND customer_license.type = 'Commercial' AND device.cplatform = 'Office 365 Exchange Online' GROUP BY customer_license.customerid ) X\G;" 2>> /dev/null)

Active_Devices1=`echo $Active_DevicesFS| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
FS_TOTAL_U=`echo $Active_DevicesFS| awk -F":" '{print $3}'`
ap1Active_Devices1=`echo $ap1Active_DevicesFS| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
ap1FS_TOTAL_U=`echo $ap1Active_DevicesFS| awk -F":" '{print $3}'`
sumActive_Devices1=`expr $Active_Devices1 + $ap1Active_Devices1`
sumFS_TOTAL_U=`bc <<< "$FS_TOTAL_U + $ap1FS_TOTAL_U"`
echo -e "Put Active_Devices EO =  $sumActive_Devices1 - in E4 cells of newly created document\n"
echo -e "Put FS_TOTAL_U EO =  $sumFS_TOTAL_U - in F4 cells of newly created document\n"
#####################################################################

Active_DevicesOD=$(insync_rds -e "SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM ( SELECT COUNT( IF( device.device_disabled = 0, 1, NULL ) ) AS Active_Devices, SUM(device.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM customer, customer_license, device, usertable WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND usertable.customerid = customer.id AND device.customerid = customer_license.customerid AND device.cloudapp_backup_initiated = True AND device.user_id = usertable.id AND customer_license.type = 'Commercial' AND device.cplatform = 'Office 365 OneDrive' GROUP BY customer_license.customerid ) X\G;" 2>> /dev/null)
ap1Active_DevicesOD=$(insync_rds_prod_D1 -e "SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM ( SELECT COUNT( IF( device.device_disabled = 0, 1, NULL ) ) AS Active_Devices, SUM(device.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM customer, customer_license, device, usertable WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND usertable.customerid = customer.id AND device.customerid = customer_license.customerid AND device.cloudapp_backup_initiated = True AND device.user_id = usertable.id AND customer_license.type = 'Commercial' AND device.cplatform = 'Office 365 OneDrive' GROUP BY customer_license.customerid ) X\G;" 2>> /dev/null)
Active_Devices1OD=`echo $Active_DevicesOD| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
FS_TOTAL_UOD=`echo $Active_DevicesOD| awk -F":" '{print $3}'`
ap1Active_Devices1OD=`echo $ap1Active_DevicesOD| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
ap1FS_TOTAL_UOD=`echo $ap1Active_DevicesOD| awk -F":" '{print $3}'`
sumActive_Devices1OD=`expr $Active_Devices1OD + $ap1Active_Devices1OD`
sumFS_TOTAL_UOD=`bc <<< "$FS_TOTAL_UOD + $ap1FS_TOTAL_UOD"`
echo -e "Put Active_Devices OD =  $sumActive_Devices1OD - in E5 cells of newly created document\n"
echo -e "Put FS_TOTAL_U OD =  $sumFS_TOTAL_UOD - in F5 cells of newly created document\n"

####################################################################
custid=$(echo -e $(insync_rds  -e "select customerid from customer_license where m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()));" 2>> /dev/null ) | cut -d " " -f2-|tr " " "," )
ap1custid=$(echo -e $(insync_rds_prod_D1  -e "select customerid from customer_license where m365_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()));" 2>> /dev/null ) | cut -d " " -f2-|tr " " "," )
####################################################################
site_collection=$(isharepoint_prod -e "SELECT COUNT( IF( share_point_site_collection.configured = 1, 1, NULL ) ) AS Active_Sites, SUM(fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM share_point_site_collection WHERE enabled = 1  and customerid in ($custid) \G;" 2>> /dev/null)
ap1site_collection=$(insync_sharepoint_rds_d1_prod -e "SELECT COUNT( IF( share_point_site_collection.configured = 1, 1, NULL ) ) AS Active_Sites, SUM(fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM share_point_site_collection WHERE enabled = 1  and customerid in ($ap1custid) \G;" 2>> /dev/null)
Active_Sites=`echo $site_collection| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
fstotaluGB=`echo $site_collection| awk -F":" '{print $3}'`
ap1Active_Sites=`echo $ap1site_collection| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
ap1fstotaluGB=`echo $ap1site_collection| awk -F":" '{print $3}'`
sumActive_Sites=`expr $Active_Sites + $ap1Active_Sites`
sumfstotaluGB=`bc <<< "$fstotaluGB + $ap1fstotaluGB"`
echo -e "Put Active_Sites =  $sumActive_Sites - in E6 cells of newly created document\n"
echo -e "Put fstotaluGB  =  $sumfstotaluGB - in F6 cells of newly created document\n"
##############################
GActive_Teams=$(iMSGroupRDS_rds -e "SELECT COUNT( IF( cloud_msteam.team.configured = 1, 1, NULL ) ) AS Active_Teams, ( SUM(cloud_msteam.team.fstotalu) + SUM( cloud_msteam.conversation_stat.fstotalu ) ) / (1024 * 1024 * 1024) AS fstotaluGB FROM cloud_msteam.team, cloud_msteam.conversation_stat WHERE cloud_msteam.team.customerid in ($custid) AND  cloud_msteam.conversation_stat.customerid in ($custid) AND  cloud_msteam.team.enabled = 1 AND cloud_msteam.conversation_stat.teamid = cloud_msteam.team.id \G;" 2>> /dev/null )
ap1GActive_Teams=$(msgrouprds-prod-D1 -e "SELECT COUNT( IF( cloud_1_msteam.team.configured = 1, 1, NULL ) ) AS Active_Teams, ( SUM(cloud_1_msteam.team.fstotalu) + SUM( cloud_1_msteam.conversation_stat.fstotalu ) ) / (1024 * 1024 * 1024) AS fstotaluGB FROM cloud_1_msteam.team, cloud_1_msteam.conversation_stat WHERE cloud_1_msteam.team.customerid in ($ap1custid) AND  cloud_1_msteam.conversation_stat.customerid in ($ap1custid) AND  cloud_1_msteam.team.enabled = 1 AND cloud_1_msteam.conversation_stat.teamid = cloud_1_msteam.team.id \G;" 2>> /dev/null )
Active_Teams=`echo $GActive_Teams| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
GfstotaluGB=`echo $GActive_Teams| awk -F":" '{print $3}'`
ap1Active_Teams=`echo $ap1GActive_Teams| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
ap1GfstotaluGB=`echo $ap1GActive_Teams| awk -F":" '{print $3}'`
sumActive_Teams=`expr $Active_Teams + $ap1Active_Teams`
sumGfstotaluGB=`bc <<< "$GfstotaluGB + $ap1GfstotaluGB"`
echo -e "Put Active_Teams =  $sumActive_Teams - in E7 cells of newly created document\n"
echo -e "Put fstotaluGB  =  $sumGfstotaluGB - in F7 cells of newly created document\n"
########################################################################

GActive_grps=$(iMSGroupRDS_rds -e "SELECT COUNT(id) FROM cloud_msgroup.groups_table WHERE configured = 1 \G;" 2>> /dev/null )
ap1GActive_grps=$(msgrouprds-prod-D1 -e "SELECT COUNT(id) FROM cloud_1_msgroup.groups_table WHERE configured = 1 \G;" 2>> /dev/null )
Active_grps=`echo $GActive_grps| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
ap1Active_grps=`echo $ap1GActive_grps| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
sumActive_grps=`expr $Active_grps + $ap1Active_grps`
echo -e "Put Active_groups =  $sumActive_grps - in E8 cells of newly created document\n"

sumus0fstotalgb=$(iMSGroupRDS_rds -e "SELECT ((SELECT SUM(fstotalu) FROM cloud_msgroup.group_metadata_stats) +(SELECT SUM(fstotalu) FROM cloud_msgroup.planner_stats) +(SELECT SUM(fstotalu) FROM cloud_msgroup.gmb_stats)) / (1024 * 1024 * 1024) AS total_in_gb \G;" 2>> /dev/null)

sumap1fstotalgb=$(msgrouprds-prod-D1 -e "SELECT ((SELECT SUM(fstotalu) FROM cloud_1_msgroup.group_metadata_stats) +(SELECT SUM(fstotalu) FROM cloud_1_msgroup.planner_stats) +(SELECT SUM(fstotalu) FROM cloud_1_msgroup.gmb_stats)) / (1024 * 1024 * 1024) AS total_in_gb \G;" 2>> /dev/null)

fsus0=`echo $sumus0fstotalgb |awk -F":" '{print $2}'`
fsap1=`echo $sumap1fstotalgb |awk -F":" '{print $2}'`
sumgrpssfstotaluGB=`bc <<< "$fsus0 + $fsap1"`
echo -e "Put FSTotalU Groups  =  $sumgrpssfstotaluGB - in F8 cells of newly created document\n"

#echo -e "Put groupsActive_Teams(update manually by seeing commands provided in doc) =  $groupssumActive_Teams - in E8 cells of newly created document\n"
#echo -e "Put groupsfstotaluGB(update manually by seeing commands provided in doc)  =  $groupssumGfstotaluGB - in F8 cells of newly created document\n"
###########################################################################

PFactive=$(capp_rds -e "SELECT Count( IF( cloud_pf.public_folder.configured = 1, 1, NULL ) ) AS Active_Folders, SUM( cloud_pf.public_folder.fstotalu ) / (1024 * 1024 * 1024) AS fstotaluGB FROM  cloud_pf.public_folder  WHERE cloud_pf.public_folder.customerid in ($custid)\G;" 2>> /dev/null )
ap1PFactive=$(insync_sharepoint_rds_d1_prod -e "SELECT Count( IF( cloud_1_pf.public_folder.configured = 1, 1, NULL ) ) AS Active_Folders, SUM( cloud_1_pf.public_folder.fstotalu ) / (1024 * 1024 * 1024) AS fstotaluGB FROM  cloud_1_pf.public_folder  WHERE cloud_1_pf.public_folder.customerid in ($ap1custid)\G;" 2>> /dev/null )
Active_folder=`echo $PFactive| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
PFfstotaluGB=`echo $PFactive| awk -F":" '{print $3}'`
ap1Active_folder=`echo $ap1PFactive| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
ap1PFfstotaluGB=`echo $ap1PFactive| awk -F":" '{print $3}'`
sumActive_folder=`expr $Active_folder + $ap1Active_folder`
sumPFfstotaluGB=`bc <<< "$PFfstotaluGB + $ap1PFfstotaluGB"`
echo -e "Put Active_Folders =  $sumActive_folder - in E9 cells of newly created document\n"
echo -e "Put fstotaluGB  =  $sumPFfstotaluGB - in F9 cells of newly created document\n"
########################################################################

GsuiteActive_Users=$(insync_rds -e "SELECT COUNT( customer.id ) AS Total_Tenants FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND customer_license.type = 'Commercial' AND customer_license.total_google_users > 0;" 2>> /dev/null)
GSActive_Users1=`echo $GsuiteActive_Users| awk -F" " '{print $2}'`
ap1GsuiteActive_Users=$(insync_rds_prod_D1 -e "SELECT COUNT( customer.id ) AS Total_Tenants FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND customer_license.type = 'Commercial' AND customer_license.total_google_users > 0;" 2>> /dev/null)
ap1GSActive_Users1=`echo $ap1GsuiteActive_Users| awk -F" " '{print $2}'`
sumGSActive_Users1=`expr $GSActive_Users1 + $ap1GSActive_Users1`
echo -e "Put GSuite Total_Tenants =  $sumGSActive_Users1 - in B10,B11,B12 cells of newly created document\n"
########################################################################

GSTotal_Lic_Users=$(insync_rds -e "SELECT SUM( customer_license.total_google_users ) AS Total_Lic_Users FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND customer_license.type = 'Commercial' AND customer_license.total_google_users > 0;" 2>> /dev/null)
ap1GSTotal_Lic_Users=$(insync_rds_prod_D1 -e "SELECT SUM( customer_license.total_google_users ) AS Total_Lic_Users FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND customer_license.type = 'Commercial' AND customer_license.total_google_users > 0;" 2>> /dev/null)
GStotallic=`echo $GSTotal_Lic_Users| awk -F" " '{print $2}'`
ap1GStotallic=`echo $ap1GSTotal_Lic_Users| awk -F" " '{print $2}'`
sumGStotallic=`expr $GStotallic + $ap1GStotallic`
echo -e "Put GSuite Total_Lic_Users =  $sumGStotallic - in C10,C11,C12 cells of newly created document\n"
########################################################################
GSTotal_act_Users=$(insync_rds -e "SELECT COUNT(product_ca_consumption.id) FROM customer, customer_license, product_ca_consumption WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND product_ca_consumption.customerid = customer.id AND customer_license.type = 'Commercial' AND product_ca_consumption.google_license_state in (1);" 2>> /dev/null)
ap1GSTotal_act_Users=$(insync_rds_prod_D1 -e "SELECT COUNT(product_ca_consumption.id) FROM customer, customer_license, product_ca_consumption WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND product_ca_consumption.customerid = customer.id AND customer_license.type = 'Commercial' AND product_ca_consumption.google_license_state in (1);" 2>> /dev/null)
GStotalsct=`echo $GSTotal_act_Users| awk -F" " '{print $2}'`
ap1GStotalsct=`echo $ap1GSTotal_act_Users| awk -F" " '{print $2}'`
sumGStotalsct=`expr $GStotalsct + $ap1GStotalsct`
echo -e "Put GSuite Total Active Users =  $sumGStotalsct - in D10,D11,D12 cells of newly created document\n"
#######################################################################

GSActive_Devices=$(insync_rds -e "SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM ( SELECT COUNT( IF( device.device_disabled = 0, 1, NULL ) ) AS Active_Devices, SUM(device.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM customer, customer_license, device, usertable WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND usertable.customerid = customer.id AND device.customerid = customer_license.customerid AND device.cloudapp_backup_initiated = True AND device.user_id = usertable.id AND customer_license.type = 'Commercial' AND device.cplatform = 'G Suite Gmail' GROUP BY customer_license.customerid ) X\G;" 2>> /dev/null)
ap1GSActive_Devices=$(insync_rds_prod_D1 -e "SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM ( SELECT COUNT( IF( device.device_disabled = 0, 1, NULL ) ) AS Active_Devices, SUM(device.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM customer, customer_license, device, usertable WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND usertable.customerid = customer.id AND device.customerid = customer_license.customerid AND device.cloudapp_backup_initiated = True AND device.user_id = usertable.id AND customer_license.type = 'Commercial' AND device.cplatform = 'G Suite Gmail' GROUP BY customer_license.customerid ) X\G;" 2>> /dev/null)
GSActive_Devices1=`echo $GSActive_Devices| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
GSFS_TOTAL_U=`echo $GSActive_Devices| awk -F":" '{print $3}'`
ap1GSActive_Devices1=`echo $ap1GSActive_Devices| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
ap1GSFS_TOTAL_U=`echo $ap1GSActive_Devices| awk -F":" '{print $3}'`
sumGSActive_Devices1=`expr $GSActive_Devices1 + $ap1GSActive_Devices1`
sumGSFS_TOTAL_U=`bc <<< "$GSFS_TOTAL_U + $ap1GSFS_TOTAL_U"`
echo -e "Put GSuite Active_Devices =  $sumGSActive_Devices1 - in E10 cells of newly created document\n"
echo -e "Put GSuite FS_TOTAL_U =  $sumGSFS_TOTAL_U - in F10 cells of newly created document\n"
########################################################################

GDActive_Devices=$(insync_rds -e "SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM ( SELECT COUNT( IF( device.device_disabled = 0, 1, NULL ) ) AS Active_Devices, SUM(device.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM customer, customer_license, device, usertable WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND usertable.customerid = customer.id AND device.customerid = customer_license.customerid AND device.cloudapp_backup_initiated = True AND device.user_id = usertable.id AND customer_license.type = 'Commercial' AND device.cplatform = 'G Suite Google Drive' GROUP BY customer_license.customerid ) X \G;" 2>> /dev/null)
ap1GDActive_Devices=$(insync_rds_prod_D1 -e "SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM ( SELECT COUNT( IF( device.device_disabled = 0, 1, NULL ) ) AS Active_Devices, SUM(device.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM customer, customer_license, device, usertable WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()) ) AND usertable.customerid = customer.id AND device.customerid = customer_license.customerid AND device.cloudapp_backup_initiated = True AND device.user_id = usertable.id AND customer_license.type = 'Commercial' AND device.cplatform = 'G Suite Google Drive' GROUP BY customer_license.customerid ) X \G;" 2>> /dev/null)

GDActive_Devices1=`echo $GDActive_Devices| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
GDFS_TOTAL_U=`echo $GDActive_Devices| awk -F":" '{print $3}'`
ap1GDActive_Devices1=`echo $ap1GDActive_Devices| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
ap1GDFS_TOTAL_U=`echo $ap1GDActive_Devices| awk -F":" '{print $3}'`
sumGDActive_Devices1=`expr $GDActive_Devices1 + $ap1GDActive_Devices1`
sumGSFS_TOTAL_U=`bc <<< "$GDFS_TOTAL_U + $ap1GDFS_TOTAL_U"`
echo -e "Put GDrive Active_Devices =  $sumGDActive_Devices1 - in E11 cells of newly created document\n"
echo -e "Put GDrive FS_TOTAL_U =  $sumGSFS_TOTAL_U - in F11 cells of newly created document\n"
#######################################################################

custidglic=$(echo -e $(insync_rds  -e "select customerid from customer_license where google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()));" 2>> /dev/null ) | cut -d " " -f2-|tr " " "," )
ap1custidglic=$(echo -e $(insync_rds_prod_D1  -e "select customerid from customer_license where google_license_expiry > ( SELECT UNIX_TIMESTAMP(NOW()));" 2>> /dev/null ) | cut -d " " -f2-|tr " " "," )
#######################################################################

Gdrive_site_collection=$(capp_rds -e "SELECT COUNT( IF( cloud_gteamdrive.team_drives_collection.configured = 1, 1, NULL ) ) AS Active_Drives, SUM( cloud_gteamdrive.team_drives_collection.fstotalu ) / (1024 * 1024 * 1024) AS fstotaluGB FROM cloud_gteamdrive.team_drives_collection WHERE cloud_gteamdrive.team_drives_collection.customerid in ($custidglic) AND  cloud_gteamdrive.team_drives_collection.enabled = 1 \G;" 2>> /dev/null)
ap1Gdrive_site_collection=$(insync_sharepoint_rds_d1_prod -e "SELECT COUNT( IF( cloud_1_gteamdrive.team_drives_collection.configured = 1, 1, NULL ) ) AS Active_Drives, SUM( cloud_1_gteamdrive.team_drives_collection.fstotalu ) / (1024 * 1024 * 1024) AS fstotaluGB FROM cloud_1_gteamdrive.team_drives_collection WHERE cloud_1_gteamdrive.team_drives_collection.customerid in ($ap1custidglic) AND  cloud_1_gteamdrive.team_drives_collection.enabled = 1 \G;" 2>> /dev/null)
Active_Drives=`echo $Gdrive_site_collection| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
GdrivefstotaluGB=`echo $Gdrive_site_collection| awk -F":" '{print $3}'`
ap1Active_Drives=`echo $ap1Gdrive_site_collection| awk -F":" '{print $2}'|awk -F" " '{print $1}'`
ap1GdrivefstotaluGB=`echo $ap1Gdrive_site_collection| awk -F":" '{print $3}'`
sumActive_Drives=`expr $Active_Drives + $ap1Active_Drives`
sumGdrivefstotaluGB=`bc <<< "$GdrivefstotaluGB + $ap1GdrivefstotaluGB"`
echo -e "Put Gsuite Shared Active_Drives =  $sumActive_Drives - in E12 cells of newly created document\n"
echo -e "Put GSuite Shared fstotaluGB  =  $sumGdrivefstotaluGB - in F12 cells of newly created document\n"
#######################################################################
EO_active_tenant=$(insync_rds -e "select count(*) from customer_license where total_device_users != 0 and from_unixtime(timelimit) > NOW();" 2>> /dev/null)
ap1EO_active_tenant=$(insync_rds_prod_D1 -e "select count(*) from customer_license where total_device_users != 0 and from_unixtime(timelimit) > NOW();" 2>> /dev/null)
EO_active_tenant1=`echo $EO_active_tenant| awk -F" " '{print $2}'`
ap1EO_active_tenant1=`echo $ap1EO_active_tenant| awk -F" " '{print $2}'`
sumEO_active_tenant1=`expr $EO_active_tenant1 + $ap1EO_active_tenant1`
echo -e "Endpoint Active Tenants  =  $sumEO_active_tenant1 \n"
#######################################################################
EO_total_lic=$(insync_rds -e "select sum(total_device_users) from customer_license where total_device_users != 0 and from_unixtime(timelimit) > NOW();" 2>> /dev/null)
ap1EO_total_lic=$(insync_rds_prod_D1 -e "select sum(total_device_users) from customer_license where total_device_users != 0 and from_unixtime(timelimit) > NOW();" 2>> /dev/null)
EO_total_lic1=`echo $EO_total_lic| awk -F" " '{print $2}'`
ap1EO_total_lic1=`echo $ap1EO_total_lic| awk -F" " '{print $2}'`
sumEO_total_lic1=`expr $EO_total_lic1 + $ap1EO_total_lic1`
echo -e "Endpoint Total Licensed Users  =  $sumEO_total_lic1 \n"
######################################################################

EO_total_user=$(insync_rds -e "select count(distinct usertable.id) from usertable, customer_license, device where usertable.customerid = customer_license.customerid and total_device_users != 0 and from_unixtime(timelimit) > NOW() and device.user_id = usertable.id and device.cplatform in ('win32', 'darwin', 'linux2', 'ios', 'android', 'windows8') and user_disabled = 0;" 2>> /dev/null)
ap1EO_total_user=$(insync_rds_prod_D1 -e "select count(distinct usertable.id) from usertable, customer_license, device where usertable.customerid = customer_license.customerid and total_device_users != 0 and from_unixtime(timelimit) > NOW() and device.user_id = usertable.id and device.cplatform in ('win32', 'darwin', 'linux2', 'ios', 'android', 'windows8') and user_disabled = 0;" 2>> /dev/null)
EO_total_user1=`echo $EO_total_user| awk -F" " '{print $3}'`
ap1EO_total_user1=`echo $ap1EO_total_user| awk -F" " '{print $3}'`
sumEO_total_user1=`expr $EO_total_user1 + $ap1EO_total_user1`
echo -e "Endpoint Total Active Users  =  $sumEO_total_user1 \n"
######################################################################

EO_total_dev=$(insync_rds -e "select count(distinct usertable.id) from usertable, customer_license, device where usertable.customerid = customer_license.customerid and total_device_users != 0 and from_unixtime(timelimit) > NOW() and device.user_id = usertable.id and device.cplatform in ('win32', 'darwin', 'linux2', 'ios', 'android', 'windows8');" 2>> /dev/null)
ap1EO_total_dev=$(insync_rds_prod_D1 -e "select count(distinct usertable.id) from usertable, customer_license, device where usertable.customerid = customer_license.customerid and total_device_users != 0 and from_unixtime(timelimit) > NOW() and device.user_id = usertable.id and device.cplatform in ('win32', 'darwin', 'linux2', 'ios', 'android', 'windows8');" 2>> /dev/null)

EO_total_dev1=`echo $EO_total_dev| awk -F" " '{print $3}'`
ap1EO_total_dev1=`echo $ap1EO_total_dev| awk -F" " '{print $3}'`
sumEO_total_dev1=`expr $EO_total_dev1 + $ap1EO_total_dev1`
echo -e "Endpoint Total Active Devices  =  $sumEO_total_dev1 \n"
#####################################################################

EO_totalFS=$(insync_rds -e "select sum(total_data_usage)/(1024*1024*1024) from device_list_view, usertable, customer_license where total_device_users !=0 and from_unixtime(timelimit) > NOW() and device_list_view.customerid = customer_license.customerid and device_list_view.cplatform in ('win32', 'darwin', 'linux2', 'ios', 'android', 'windows8') and device_list_view.user_id = usertable.id and user_disabled = 0;" 2>> /dev/null)
ap1EO_totalFS=$(insync_rds_prod_D1 -e "select sum(total_data_usage)/(1024*1024*1024) from device_list_view, usertable, customer_license where total_device_users !=0 and from_unixtime(timelimit) > NOW() and device_list_view.customerid = customer_license.customerid and device_list_view.cplatform in ('win32', 'darwin', 'linux2', 'ios', 'android', 'windows8') and device_list_view.user_id = usertable.id and user_disabled = 0;" 2>> /dev/null)

EO_totalFS1=`echo $EO_totalFS| awk -F" " '{print $2}'`
ap1EO_totalFS1=`echo $ap1EO_totalFS| awk -F" " '{print $2}'`
sumEO_totalFS1=`bc <<< "$EO_totalFS1 + $ap1EO_totalFS1"`
echo -e "Endpoint FSTotalU in GB  =  $sumEO_totalFS1 \n"



echo "FINISH"

