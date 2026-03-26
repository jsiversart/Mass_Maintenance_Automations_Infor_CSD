COUNT_FLAG_SQL = """
SELECT 
    whse,
    pwh.prod
FROM 
    default.icsw pwh
INNER JOIN 
    default.icsp prod ON prod.prod = pwh.prod AND prod.cono = pwh.cono
WHERE
    pwh.cono = 1
    AND (
        pwh.whse != '25'
        OR (pwh.whse = '25' AND pwh.prodline LIKE '%LK')
    )
    AND prod.descrip_2 LIKE ('%REPLD%')
    AND pwh.statustype <> 'L'
    AND pwh.qtyonhand - pwh.qtyreservd - pwh.qtycommit > 0
    AND pwh.whse NOT LIKE '%C'
ORDER BY pwh.prod, whse;
"""

DNR_SQL_1 =  """
SELECT pwh.whse, pwh.prod
FROM default.icsw pwh
INNER JOIN default.icsp prod ON prod.prod = pwh.prod AND prod.cono = pwh.cono AND prod.statustype <> 'I'
WHERE
    pwh.cono = 1
    AND UPPER(prod.statustype) <> 'I'
    AND (prod.descrip_2 = 'NO LONGER AVAILABLE' OR prod.descrip_2 like 'REPLD BY%')
    AND UPPER(pwh.statustype) <> 'X'
    AND GREATEST(qtyonhand+qtyonorder-qtyreservd-qtycommit-qtybo-qtydemand,0) = 0
    AND pwh.prod in 
    	(select prod from default.icsw where (prodline like '%LK' or prodline like 'MISC') and cono = 1 and whse = '25' and 
    	GREATEST(qtyonhand+qtyonorder-qtyreservd-qtycommit-qtybo-qtydemand,0) = 0) AND 
    	pwh.prod in (select prod from default.icsw where cono = 1 and whse = '50' and 
    	GREATEST(qtyonhand+qtyonorder-qtyreservd-qtycommit-qtybo-qtydemand,0) = 0)
	    AND (
      SELECT COUNT(DISTINCT whse)
      FROM default.icsw
      WHERE cono = 1
        AND prod = pwh.prod
        AND GREATEST(qtyonhand + qtyonorder - qtyreservd - qtycommit - qtybo - qtydemand, 0) > 0
        ) <= 5
          AND (
          prod.descrip_2 = 'NO LONGER AVAILABLE'
          OR (
              prod.descrip_2 LIKE 'REPLD BY%'
              AND EXISTS (
                  SELECT 1 FROM default.icsec s
                  WHERE s.cono = 1
                    AND UPPER(s.rectype) = 'P'
                    AND s.prod = pwh.prod
              )
          )
      )
"""
DNR_SQL_2 = """SELECT pwh.whse, pwh.prod
, prod.descrip_2, pwh.statustype, pwh.prodline
FROM default.icsw pwh
INNER JOIN default.icsp prod ON prod.prod = pwh.prod AND prod.cono = pwh.cono AND prod.statustype <> 'I'
WHERE
    pwh.cono = 1
    AND UPPER(prod.statustype) <> 'I'
    AND (prod.descrip_2 = 'NO LONGER AVAILABLE' OR prod.descrip_2 like 'REPLD BY%')
    AND UPPER(pwh.statustype) <> 'X'
    AND GREATEST(qtyonhand+qtyonorder-qtyreservd-qtycommit-qtybo-qtydemand,0) = 0
    AND pwh.prod in 
    	(select prod from default.icsw where prodline NOT like '%LK' and prodline NOT like 'MISC' and cono = 1 and whse = '25' and 
    	GREATEST(qtyonhand+qtyonorder-qtyreservd-qtycommit-qtybo-qtydemand,0) = 0) AND 
    	pwh.prod in (select prod from default.icsw where cono = 1 and whse = '50' and 
    	GREATEST(qtyonhand+qtyonorder-qtyreservd-qtycommit-qtybo-qtydemand,0) = 0)
    AND (
          SELECT COUNT(DISTINCT whse)
          FROM default.icsw
          WHERE cono = 1
            AND prod = pwh.prod
            AND GREATEST(qtyonhand + qtyonorder - qtyreservd - qtycommit - qtybo - qtydemand, 0) > 0
      ) <= 5
    AND (
    prod.descrip_2 = 'NO LONGER AVAILABLE'
          OR (
              prod.descrip_2 LIKE 'REPLD BY%'
              AND EXISTS (
                  SELECT 1 FROM default.icsec s
                  WHERE s.cono = 1
                    AND UPPER(s.rectype) = 'P'
                    AND s.prod = pwh.prod
              )
          )
      )"""

ICSL_COMPASS = """
        select     
        *
        from default.icsl
        WHERE cono = 1"""

ICSL_SQLITE = """
    WITH INNER AS (select
    whse,
    CAST(d.vendno AS INT) as vendno,
    prodline,
    class,
    '1' as new_class,
    cast(cast(safeallamt as int) as text) as safeallamt,
    '0' as new_safeallamt,
    rolloanusagefl,
    'true' AS new_rolloanusagefl, 
    ordcalcty,
    CASE WHEN w.type = 'D' then 'E' else 'M' END AS new_ordcalcty,
    lower(d.usagectrl) as usagectrl,
    usgmths, -- see above
    frozentype, --always blank
    '' AS new_frozentype,
    frozenmos, --always 0
    '0' AS new_frozenmos,
    arptype,
    CASE WHEN e.value is null then 
            CASE WHEN w.Type = 'D' then 'V' else 'W' END
        WHEN e.impacted_whse = 'ALL' then
            CASE WHEN e.arp_change = 'IGNORE' then d.arptype
            ELSE e.arp_change
            END
        WHEN d.whse like '%C' THEN d.arptype
            WHEN (',' || e.impacted_whse || ',') LIKE '%,' || CAST(d.whse as INT) || ',%' THEN
                CASE WHEN e.arp_change = 'IGNORE' then d.arptype
                ELSE e.arp_change
                END
            ELSE d.arptype
        END AS new_arptype,
    d.arpwhse, --by whse
    arppushfl,
    'true' AS new_arppushfl, 
    CASE WHEN w.Type is 'D' THEN '' 
        WHEN W.Type is null THEN '25'
        ELSE CAST(CAST(w.arpwhse AS REAL) AS INTEGER) END as default_arpwhse,
    CASE WHEN prodline = 'SEASNL' then 'Y'
        WHEN r.usagectrl = 'f' THEN 'Y'
        ELSE 'N' end as seasonal_fl
    from icsl_audit_data d
    left join whseinfo w on d.whse = w.Warehouse  --get w.Arpwhse and w.Type (B branch or D DC)
    left join icsw_usagectrl_rules r on d.vendno = r.vendno
    left join icsw_arppath_exceptions e
        ON
            (e.type = 'prodline' AND e.value = d.prodline)
            OR
            (e.type = 'arpvendno' AND e.value = d.vendno)
            ),
    OUTER AS (SELECT 
        whse,
        vendno,
        prodline,
        class,
        new_class,
        safeallamt,
        new_safeallamt,
        rolloanusagefl,
        new_rolloanusagefl, 
        ordcalcty,
        new_ordcalcty,
        usagectrl,
        CASE WHEN seasonal_fl = 'Y' THEN 'f' else 'b' END AS new_usagectrl,
        usgmths,
        CASE WHEN seasonal_fl = 'Y' THEN '3' else '6' END AS new_usgmths, 
        frozentype, 
        new_frozentype,
        frozenmos,
        new_frozenmos,
        arptype,
        new_arptype,
        arpwhse,
        CASE WHEN new_arptype = 'V' THEN ''
            ELSE default_arpwhse END AS new_arpwhse,
        arppushfl,
        new_arppushfl
    from INNER)
    SELECT * FROM OUTER 
    WHERE
            COALESCE(TRIM(rolloanusagefl), '') <> COALESCE(TRIM(new_rolloanusagefl), '')
            OR COALESCE(TRIM(ordcalcty), '') <> COALESCE(TRIM(new_ordcalcty), '')
            OR COALESCE(TRIM(class), '') <> COALESCE(TRIM(new_class), '')
            OR COALESCE(TRIM(safeallamt), '') <> COALESCE(TRIM(new_safeallamt), '')
            OR COALESCE(TRIM(usagectrl), '') <> COALESCE(TRIM(new_usagectrl), '')
            OR COALESCE(TRIM(usgmths), '') <> COALESCE(TRIM(new_usgmths), '')
            OR COALESCE(TRIM(frozentype), '') <> COALESCE(TRIM(new_frozentype), '')
            OR COALESCE(TRIM(frozenmos), '') <> COALESCE(TRIM(new_frozenmos), '')
            OR COALESCE(TRIM(arptype), '') <> COALESCE(TRIM(new_arptype), '')
            OR COALESCE(TRIM(arpwhse), '') <> COALESCE(TRIM(new_arpwhse), '')
            OR COALESCE(TRIM(arppushfl), '') <> COALESCE(TRIM(new_arppushfl), '')
    """

WHSE_RANK_QUERY =  """
WITH xref as 
	(SELECT DISTINCT prod AS oldno, 
       altprod AS prod
FROM default.icsec
WHERE UPPER(rectype) = 'P' 
AND cono = 1),
ranknow as (select prod, whse, whserank from default.icsw where cono =1)
SELECT 
x.prod,
w.whse
FROM default.icsw w
INNER JOIN xref x ON w.prod = x.oldno
LEFT JOIN ranknow r ON r.prod = x.prod AND r.whse = w.whse
WHERE cono = 1 
	AND whse not in ('00','01','03C','04C','05C','13','25','35')
    AND upper(r.whserank) <> 'E'
	AND GREATEST(w.qtyonhand + w.qtyonorder - w.qtyreservd - w.qtycommit - w.qtybo - w.qtydemand,0) > 0
ORDER BY prod asc
"""