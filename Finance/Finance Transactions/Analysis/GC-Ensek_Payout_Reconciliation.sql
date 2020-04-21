
; with cte_payouts as (
select *
from aws_fin_stage1_extracts.fin_go_cardless_api_payouts
where
  substring (created_at, 1, 10) between '2020-01-01' and '2020-03-31'
      )


, cte_events_payout as (
       select *,
       substring(created_at, 1, 7) as payout_month
      from aws_fin_stage1_extracts.fin_go_cardless_api_events events_a
      where events_a.resource_type = 'payouts'
        and substring(events_a.created_at, 1, 10) between '2020-01-01' and '2020-03-31'
      )



, cte_ensek as (
        select *,
       substring(createddate, 1, 7) as payout_month
        from aws_fin_stage1_extracts.fin_sales_ledger_all_time
        where nominal = '7603'
        and substring(createddate, 1, 10) between '2020-01-01' and '2020-03-31' --- dateadd(day, -0, '2020-02-01') and  dateadd(day, 0, '2020-02-29')
      )




, cte_payments as (
---- Payments -----

select distinct events.id,
                events.created_at                       as events_created_at,
                events.resource_type,
                events.action,
                events.customer_notifications,
                events.cause,
                events.description,
                events.origin,
                events.reason_code,
                events.scheme,
                events.will_attempt_retry,
                events.mandate                          as events_mandate,
                events.new_customer_bank_account,
                events.new_mandate,
                events.organisation,
                events.parent_event,
                events.payment,
                events.payout,
                events.previous_customer_bank_account,
                events.refund,
                events.subscription,
                payments.id                             as paymentID,
                payments.amount :: float / 100          as amount,
                payments.amount_refunded :: float / 100 as amount_refunded,
                payments.created_at,
                payments.charge_date,
                payments.status,
                man.mandate_id,
                payments.description,
                payments.reference,
                payments.payout,
                payouts.payout_id ,
                events_payout.payout_month ,
                client.client_id                        as customers_id,
                lkp.igl_acc_id                          as ensekAccountId
from cte_events_payout as events_payout
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_events events on events_payout.id = events.parent_event
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_payments payments on payments.id = events.payment
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_payouts payouts
         on payouts.payout_id = events_payout.payout
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
         on man.mandate_id = payments.mandate --- goCardless CLIENTS ---
       left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
         on client.client_id = man.customerid --- ref_cdb_users ---
       left join public.vw_gocardless_customer_id_mapping lkp on lkp.client_id = client.client_id
where events.action = 'paid_out'
      )

, cte_refundsSettled as (
---- Event-Refunds RefundSettled -----

select distinct events.id,
                events.created_at             as events_created_at,
                events.resource_type,
                events.action,
                events.customer_notifications,
                events.cause,
                events.description,
                events.origin,
                events.reason_code,
                events.scheme,
                events.will_attempt_retry,
                events.mandate                as events_mandate,
                events.new_customer_bank_account,
                events.new_mandate,
                events.organisation,
                events.parent_event,
                events.payment,
                events.payout,
                events.previous_customer_bank_account,
                events.refund,
                events.subscription,
                refunds.id                    as refundID,
                refunds.amount :: float / 100 as amount,
                refunds.payment               as paymentID,
                refunds.created_at            as refunds_created_at,
                payments.status               as payment_status,
                payments.payout               as payoutID,
                payouts.payout_id,
                events_payout.payout_month ,
                man.mandate_id,
                client.client_id              as customers_id,
                lkp.igl_acc_id                as ensekAccountId
from cte_events_payout as events_payout
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_events events on events_payout.id = events.parent_event
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_refunds refunds on events.refund = refunds.id
       left join aws_fin_stage1_extracts.fin_go_cardless_api_payments payments on refunds.payment = payments.id
       left join aws_fin_stage1_extracts.fin_go_cardless_api_payouts payouts on payouts.payout_id = events_payout.payout
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
         on man.mandate_id = refunds.mandate --- goCardless CLIENTS ---
       left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
         on client.client_id = man.customerid --- ref_cdb_users ---
       left join public.vw_gocardless_customer_id_mapping lkp on lkp.client_id = client.client_id
where events.action = 'refund_settled'
      )


, cte_lateFailure as (
---- Event-Payments late_failure_settled-----

select distinct events.id,
                events.created_at                       as events_created_at,
                events.resource_type,
                events.action,
                events.customer_notifications,
                events.cause,
                events.description,
                events.origin,
                events.reason_code,
                events.scheme,
                events.will_attempt_retry,
                events.mandate                          as events_mandate,
                events.new_customer_bank_account,
                events.new_mandate,
                events.organisation,
                events.parent_event,
                events.payment,
                events.payout,
                events.previous_customer_bank_account,
                events.refund,
                events.subscription,
                payments.id                             as paymentID,
                payments.amount :: float / 100          as amount,
                payments.amount_refunded :: float / 100 as amount_refunded,
                payments.created_at,
                payments.charge_date,
                payments.status,
                man.mandate_id,
                payments.description,
                payments.reference,
                payments.payout,
                payouts.payout_id,
                events_payout.payout_month ,
                client.client_id                        as customers_id,
                lkp.igl_acc_id                          as ensekAccountId
from cte_events_payout as events_payout
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_events events on events_payout.id = events.parent_event
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_payments payments on payments.id = events.payment
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_payouts payouts
         on payouts.payout_id = events_payout.payout
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
         on man.mandate_id = payments.mandate --- goCardless CLIENTS ---
       left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
         on client.client_id = man.customerid --- ref_cdb_users ---
       left join public.vw_gocardless_customer_id_mapping lkp on lkp.client_id = client.client_id
where events.action = 'late_failure_settled'
      )




, cte_chargeBack as (
---- Event-Payments chargeback_settled-----

select distinct events.id,
                events.created_at                       as events_created_at,
                events.resource_type,
                events.action,
                events.customer_notifications,
                events.cause,
                events.description,
                events.origin,
                events.reason_code,
                events.scheme,
                events.will_attempt_retry,
                events.mandate                          as events_mandate,
                events.new_customer_bank_account,
                events.new_mandate,
                events.organisation,
                events.parent_event,
                events.payment,
                events.payout,
                events.previous_customer_bank_account,
                events.refund,
                events.subscription,
                payments.id                             as paymentID,
                payments.amount :: float / 100          as amount,
                payments.amount_refunded :: float / 100 as amount_refunded,
                payments.created_at,
                payments.charge_date,
                payments.status,
                man.mandate_id,
                payments.description,
                payments.reference,
                payments.payout,
                payouts.payout_id,
                events_payout.payout_month ,
                client.client_id                        as customers_id,
                lkp.igl_acc_id                          as ensekAccountId
from cte_events_payout as events_payout
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_events events on events_payout.id = events.parent_event
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_payments payments on payments.id = events.payment
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_payouts payouts
         on payouts.payout_id = events_payout.payout
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
         on man.mandate_id = payments.mandate --- goCardless CLIENTS ---
       left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
         on client.client_id = man.customerid --- ref_cdb_users ---
       left join public.vw_gocardless_customer_id_mapping lkp on lkp.client_id = client.client_id
where events.action = 'chargeback_settled'
      )




, cte_fundsReturned as (
---- Event-Refunds -- FundsReturned -----

select distinct events.id,
                events.created_at             as events_created_at,
                events.resource_type,
                events.action,
                events.customer_notifications,
                events.cause,
                events.description,
                events.origin,
                events.reason_code,
                events.scheme,
                events.will_attempt_retry,
                events.mandate                as events_mandate,
                events.new_customer_bank_account,
                events.new_mandate,
                events.organisation,
                events.parent_event,
                events.payment,
                events.payout,
                events.previous_customer_bank_account,
                events.refund,
                events.subscription,
                refunds.id                    as refundID,
                refunds.amount :: float / 100 as amount,
                refunds.payment               as paymentID,
                refunds.created_at            as refunds_created_at,
                payments.status               as payment_status,
                payments.payout               as payoutID,
                payouts.payout_id,
                events_payout.payout_month ,
                man.mandate_id,
                client.client_id              as customers_id,
                lkp.igl_acc_id                as ensekAccountId
from cte_events_payout as events_payout
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_events events on events_payout.id = events.parent_event
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_refunds refunds on events.refund = refunds.id
       left join aws_fin_stage1_extracts.fin_go_cardless_api_payments payments on refunds.payment = payments.id
       left join aws_fin_stage1_extracts.fin_go_cardless_api_payouts payouts on payouts.payout_id = events_payout.payout
       inner join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
         on man.mandate_id = refunds.mandate --- goCardless CLIENTS ---
       left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
         on client.client_id = man.customerid --- ref_cdb_users ---
       left join public.vw_gocardless_customer_id_mapping lkp on lkp.client_id = client.client_id
where events.action = 'funds_returned'
      )



,  cte_payments_summ as (
       select
        ensekAccountId,
        substring(created_at, 1, 10) as created_at,
        payout_month ,
        amount,
        count(*) as Countif
        from cte_payments
       where (ensekAccountId is not null)
       group by
        ensekAccountId,
        payout_month ,
        created_at ,
        amount
      )

,  cte_refunds_summ as (
       select
        ensekAccountId,
        substring(refunds_created_at, 1, 10) as created_at,
        payout_month ,
        amount,
        count(*) as Countif
        from cte_refundsSettled
       where (ensekAccountId is not null)
       group by
        ensekAccountId,
        payout_month ,
        created_at ,
        amount
      )

, cte_ensek_summ as (
        select
        AccountId,
        CreatedDate,
        payout_month,
        TransAmount,
        count(*) as Countif
        from cte_ensek
        where (AccountId is not null)
       group by
        AccountId,
        payout_month,
        CreatedDate,
        TransAmount
      )


,  cte_chargeBack_summ as (
       select
        ensekAccountId,
        substring(created_at, 1, 10) as created_at,
        payout_month ,
        amount,
        count(*) as Countif
        from cte_chargeBack
       where (ensekAccountId is not null)
       group by
        ensekAccountId,
        payout_month ,
        created_at ,
        amount
      )


,  cte_lateFailure_summ as (
       select
        ensekAccountId,
        substring(created_at, 1, 10) as created_at,
        payout_month ,
        amount,
        count(*) as Countif
        from cte_lateFailure
       where (ensekAccountId is not null)
       group by
        ensekAccountId,
        payout_month ,
        created_at ,
        amount
      )


,  cte_fundsReturned_summ as (
       select
        ensekAccountId,
        substring(refunds_created_at, 1, 10) as created_at,
        payout_month ,
        amount,
        count(*) as Countif
        from cte_fundsReturned
       where (ensekAccountId is not null)
       group by
        ensekAccountId,
        payout_month ,
        created_at ,
        amount
      )




, cte_GoCardless_payments_tab as (
    select
     gc.ensekAccountId ,
     gc.created_at,
     gc.payout_month ,
     gc.amount,
     ensek.TransAmount,
     nvl(ensek.Countif, 0) as PaymentInEnsekFlag
    from cte_payments gc
    left join cte_ensek_summ ensek
        on gc.ensekAccountId = ensek.AccountId and
           gc.amount = ensek.TransAmount and
           gc.payout_month = ensek.payout_month

    )

, cte_GoCardless_refunds_tab as (
    select
     gc.ensekAccountId ,
     gc.refunds_created_at as created_at,
     gc.payout_month,
     gc.amount,
     ensek.TransAmount,
     nvl(ensek.Countif, 0) as RefundInEnsekFlag
    from cte_refundsSettled gc
    left join cte_ensek_summ ensek
        on gc.ensekAccountId = ensek.AccountId and
           (gc.amount * -1.0) = ensek.TransAmount and
            gc.payout_month = ensek.payout_month and
            ensek.TransAmount < 0 ---- REFUNDS ONLY ----
    )


,   cte_ensek_payments_tab as (
      select
       ensek.AccountId ,
       ensek.CreatedDate,
       ensek.payout_month,
       ensek.TransAmount,
       ref.amount as refundAmount,
       paym.amount as paymentAmount ,
       paym.Countif as C_Pay,
       ref.Countif as C_Ref,
       CASE WHEN coalesce(paym.Countif, ref.Countif, 0) = 0
            THEN 0
            ELSE 1
      END as inGCFlag
      from cte_ensek ensek
      left join cte_refunds_summ ref
          on ref.ensekAccountId = ensek.AccountId and
             (ref.amount * -1.0) = ensek.TransAmount and
              ref.payout_month = ensek.payout_month and
              ensek.TransAmount < 0 ---- REFUNDS ONLY ----
      left join cte_payments_summ paym
          on paym.ensekAccountId = ensek.AccountId and
             paym.amount = ensek.TransAmount and
             paym.payout_month = ensek.payout_month
 )




, cte_GoCardless_chargeBack_tab as (
    select
     gc.ensekAccountId ,
     gc.created_at,
     gc.payout_month,
     gc.amount,
     ensek.TransAmount,
     nvl(ensek.Countif, 0) as PaymentInEnsekFlag
    from cte_chargeBack gc
    left join cte_ensek_summ ensek
        on gc.ensekAccountId = ensek.AccountId and
           gc.amount = ensek.TransAmount and
           gc.payout_month = ensek.payout_month

    )


, cte_GoCardless_lateFailure_tab as (
    select
     gc.ensekAccountId ,
     gc.created_at,
     gc.payout_month = ensek.payout_month,
     gc.amount,
     ensek.TransAmount,
     nvl(ensek.Countif, 0) as PaymentInEnsekFlag
    from cte_lateFailure gc
    left join cte_ensek_summ ensek
        on gc.ensekAccountId = ensek.AccountId and
           gc.amount = ensek.TransAmount and
           gc.payout_month = ensek.payout_month

    )


, cte_GoCardless_fundsReturned_tab as (
    select
     gc.ensekAccountId ,
     gc.refunds_created_at as created_at,
    gc.payout_month ,
     gc.amount,
     ensek.TransAmount,
     nvl(ensek.Countif, 0) as RefundInEnsekFlag
    from cte_fundsReturned gc
    left join cte_ensek_summ ensek
        on gc.ensekAccountId = ensek.AccountId and
           (gc.amount * -1.0) = ensek.TransAmount and
           gc.payout_month = ensek.payout_month and
            ensek.TransAmount < 0 ---- REFUNDS ONLY ----
    )





, cte_report_summary as (

SELECT
distinct
nvl(GC.ensekAccountId, ensek.accountid ) as ensekAccountId,
nvl(GC.payout_month, ensek.payout_month) as payout_month ,
GC.GCCreatedAtDate,
GC.GCAmount,
GC.PaymentInEnsekFlag as Payment_In_Ensek_Flag,
GC.RefundInEnsekFlag as Refund_In_Ensek_Flag ,
ensek.createddate as EnsekCreatedAtDate,
ensek.transamount as EnsekAmount,
ensek.inGCFlag as Ensek_In_GC_Flag,
ensek.C_Ref,
ensek.C_Pay
FROM
      (
        Select
        pay.ensekAccountId,
        pay.created_at as GCCreatedAtDate,
        pay.payout_month,
        pay.amount as GCAmount,
        pay.PaymentInEnsekFlag ,
        0 as RefundInEnsekFlag
        from cte_GoCardless_payments_tab pay

        UNION

        select
        ref.ensekAccountId,
        ref.created_at as GCCreatedAtDate,
        ref.payout_month,
        (ref.amount * -1.0) as GCAmount,
        0 as PaymentInEnsekFlag ,
        ref.RefundInEnsekFlag
        from cte_GoCardless_refunds_tab ref
       ) GC
full outer join cte_ensek_payments_tab ensek
        on ensek.accountid = GC.ensekAccountId
       and ensek.transamount = GC.GCAmount
       and ensek.payout_month = GC.payout_month
order by 1

    )


, cte_summary_check as (select sum(CASE
                                           WHEN gcamount > 0 THEN gcamount
                                           ELSE 0
                                       END) as GCTotal_Payment,
                               sum(CASE
                                           WHEN gcamount < 0 THEN gcamount
                                           ELSE 0
                                       END) as GCTotal_Refund,
                               sum(CASE
                                           WHEN EnsekAmount > 0 THEN EnsekAmount
                                           ELSE 0
                                       END) as Ensek_Payment,
                               sum(CASE
                                           WHEN EnsekAmount < 0 THEN EnsekAmount
                                           ELSE 0
                                       END) as Ensek_Refund
                        from cte_report_summary
      )



select * from cte_report_summary
order by 1
;

 


