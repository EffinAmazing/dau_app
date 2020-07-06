view: dau {
  derived_table: {
    sql:
      with
      daily_user_activity as (
      --roll up all activity to one row per day for each user (when they trigger the chosen event on that day)
        select
          user_id,
          date(timestamp) as day
        from ${tracks.SQL_TABLE_NAME}
        where {% condition event %} event {% endcondition %}
        group by 1, 2
      ),
      active_users as (
      --create table of unique users and assign first and last active day
        select
          user_id,
          min(day) as first_active_day,
          max(day) as last_active_day
        from daily_user_activity
        group by 1
      ),
      daily_user_matrix as (
      --create one row per user/day combination using a cross join
      --only create rows for users starting on their first active day and forward
        select
          d.day,
          u.user_id,
          u.first_active_day,
          u.last_active_day,
          date_diff(d.day, u.first_active_day, day) as days_since_first_active
        from ${days.SQL_TABLE_NAME} as d, active_users as u
        where d.day >= u.first_active_day
      )
      --join all activity to daily user matrix to assign days_since_last_active and active days in 7 and 28 day windows
        select
          d.day,
          d.user_id,
          d.first_active_day,
          d.last_active_day,
          d.days_since_first_active,
          min(date_diff(d.day, a.day, day)) as days_since_last_active,
          count(distinct case when date_diff(d.day, a.day, day) < 7 then a.day end) as days_active_7d_window,
          count(distinct case when date_diff(d.day, a.day, day) < 28 then a.day end) as days_active_28d_window
        from daily_user_matrix as d
          left join daily_user_activity a on a.user_id = d.user_id
        where a.day <= d.day
        group by 1,2,3,4,5
    ;;
  }

  #
  # FILTER
  #

  filter: event {
    type: string
    suggestions: [
      "DID_SOMETHING",
      "USED_FEATURE",
      "OPENED_APP"
    ]
  }

  #
  # KEYS
  #

  dimension: pk {
    hidden: yes
    primary_key: yes
    sql: concat(cast(${day} as string), ${user_id}) ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }


  #
  # DIMENSIONS
  #

  dimension: day {
    type: date
    sql: timestamp(${TABLE}.day) ;;
  }

  dimension_group: first_active {
    timeframes: [date, week, month]
    type: time
    sql: timestamp(${TABLE}.first_active_day) ;;
  }

  dimension_group: last_active {
    timeframes: [date, week, month]
    type: time
    sql: timestamp(${TABLE}.last_active_day) ;;
  }

  dimension: days_since_first_active {
    type: number
    sql: ${TABLE}.days_since_first_active ;;
  }

  dimension: 7_day_windows_since_first_active {
    type: number
    sql: floor(${days_since_first_active}/7.00) ;;
  }

  dimension: 28_day_windows_since_first_active {
    type: number
    sql: floor(${days_since_first_active}/28.00) ;;
  }

  dimension: days_since_last_active {
    type: number
    sql: ${TABLE}.days_since_last_active ;;
  }

  dimension: days_active_7d_window {
    type: number
    sql: ${TABLE}.days_active_7d_window ;;
  }

  dimension: days_active_28d_window {
    type: number
    sql: ${TABLE}.days_active_28d_window ;;
  }

  dimension: days_active_28d_tier {
    type: tier
    style: integer
    tiers: [1,2,3,7,14,21]
    sql: ${days_active_28d_window} ;;
  }


  #
  # MEASURES
  #

  measure: dau {
    label: "DAU"
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: days_since_last_active
      value: "0"
    }
  }

  measure: wau {
    label: "WAU"
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: days_since_last_active
      value: "<7"
    }
  }

  measure: mau {
    label: "MAU"
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: days_since_last_active
      value: "<28"
    }
  }

  measure: new_dau {
    label: "New DAU"
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: days_since_last_active
      value: "0"
    }
    filters: {
      field: days_since_first_active
      value: "0"
    }
  }

  measure: new_wau {
    label: "New WAU"
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: days_since_last_active
      value: "<7"
    }
    filters: {
      field: days_since_first_active
      value: "<7"
    }
  }

  measure: new_mau {
    label: "New MAU"
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: days_since_last_active
      value: "<28"
    }
    filters: {
      field: days_since_first_active
      value: "<28"
    }
  }

  measure: percent_new_dau {
    type: number
    label: "% New DAU"
    value_format_name: percent_0
    sql: ${new_dau}/${dau} ;;
  }

  measure: percent_new_wau {
    type: number
    label: "% New WAU"
    value_format_name: percent_0
    sql: ${new_wau}/${wau} ;;
  }

  measure: percent_new_mau {
    type: number
    label: "% New MAU"
    value_format_name: percent_0
    sql: ${new_mau}/${mau} ;;
  }

}
