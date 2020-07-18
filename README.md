# Block Setup

This block is designed to be used with Segment data collected through the Segment track method and attributed to a `user_id`. After some quick setup you'll be able to see daily, weekly, and monthly active users for any event you're tracking to Segment. You can also analyze the frequency of engagement and retention of users on the tracked feature in your site or app.

1. Enter your Segment schema name into the [manifest](/projects/dau_app/files/manifest.lkml).
2. Input the `event` strings for the events you want to analyze as active events into the [event filter in the DAU view](/projects/dau_app/files/dau.view.lkml?line=58). This filter uses the `event` field from the Segment `tracks` table, so events will be in snake_case.
3. The block creates DAU tables starting Jan 1, 2020 by default, if you want to look back further, adjust [the dates PDT](/projects/dau_app/files/days.view.lkml) generation to begin on an earlier date. Since the dates view is cross-joined with all active users, pulling in too much data for high-volume apps will run slowly.
4. Open the [App DAU Dashboard](/dashboards/dau_app::dau_app) to see examples of what can be done with this block. You might want to add a default value for the Active Event filter in this dashboard, [that can be done here](/projects/dau_app/files/dau_app.dashboard.lookml?line=735).
5. Explore your data in the [DAU App - DAU explore](/explore/dau_app/dau).

You can see the screenshots of the explore and default dashboard in the [Looker blocks directory](https://looker.com/platform/blocks/source/app-daily-active-users-by-segment).
