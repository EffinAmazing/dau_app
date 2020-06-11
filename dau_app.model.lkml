connection: "st"

include: "*.view.lkml"

explore: dau {
  label: "DAU"
  view_label: "DAU"
  join: users {
    sql_on: ${users.id} = ${dau.user_id} ;;
    relationship: many_to_one
  }
}
