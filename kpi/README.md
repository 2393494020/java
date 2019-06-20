# hive

10.17.35.66  
zhanghc  
5gai@66

4.15 ~ 5.12
```
kinit -kt /home/zhanghc/zhanghc.keytab zhanghc/gsta@GSTA.COM

use noce;show create table agg_wireless_kpi_cell_h
use noce;show create table dim_sector

/DATA/PUBLIC/NOCE/AGG/AGG_WIRELESS_KPI_CELL_H/hour=2019041500
/DATA/PUBLIC/NOCE/DIM/DIM_SECTOR/day=20190513

/DATA/PUBLIC/WID/WID_DWPRB_X_JC_DAY
/DATA/PUBLIC/WID/WID_DWPRB_X_JC_WEEK
```

```sql
hive -e "use noce;select enodebid,cellid,rrccon_succrate from AGG_WIRELESS_KPI_CELL_H where hour='2019041500' and enodebid='860900' and cellid='2'"
```