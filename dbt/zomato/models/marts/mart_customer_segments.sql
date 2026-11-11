-- RFM segments with the action a CRM team would take for each
select
    s.*,
    case s.segment
        when 'Champions'   then 'Reward: early access, Gold upgrade offers'
        when 'Loyal'       then 'Upsell: new cuisines, bigger baskets'
        when 'New'         then 'Onboard: second-order coupon within 7 days'
        when 'At risk'     then 'Win back: personalised discount on favourite restaurant'
        when 'Hibernating' then 'Re-activate: strong coupon or let lapse'
        else                    'Nurture: timed lunch/dinner nudges'
    end as recommended_action
from {{ source('gold', 'customer_segments') }} s
