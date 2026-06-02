import json
import os
import sys

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "output")


def load_json(fname):
    path = os.path.join(OUTPUT_DIR, fname)
    if not os.path.exists(path):
        print(f"  [ERROR] {fname} not found. Run the analysis first.")
        return None
    with open(path) as f:
        return json.load(f)


def print_header(title):
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)


def show_popular_stations():
    data = load_json("popular_stations.json")
    if not data:
        return

    print_header("Popular Stations (All-Time Top 15)")
    for i, s in enumerate(data["alltime_top30"][:15], 1):
        print(f"  {i:2}. {s['station_name'][:45]:45} {s['rides']:>12,} rides "
              f"(ends: {s['end_count']:>12,}, starts: {s['start_count']:>12,})")

    print_header("Start-vs-End Asymmetry (Top 5 End-Heavy)")
    monthly = data["by_month"]
    latest = sorted(monthly.keys())[-1] if monthly else None
    if latest:
        print(f"  Latest month: {latest}")
        stations = sorted(monthly[latest], key=lambda x: x["end_count"] - x["start_count"], reverse=True)[:5]
        for s in stations:
            diff = s["end_count"] - s["start_count"]
            direction = "ends > starts" if diff > 0 else "starts > ends"
            print(f"  {s['station_name'][:45]:45} diff={diff:>+6,} ({direction})")


def show_bike_type_trends():
    data = load_json("bike_type_trends.json")
    if not data:
        return

    print_header("Bike Type Monthly Trends")
    print(f"  {'Month':<8} {'Total':>10} {'Classic':>10} {'Electric':>10} {'Electric %':>10}")
    print(f"  {'-'*8} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
    for row in data["monthly_summary"]:
        total = row["rides"]
        classic = row.get("classic_bike", 0)
        electric = row.get("electric_bike", 0)
        pct = electric / total * 100 if total > 0 else 0
        print(f"  {row['month']:<8} {total:>10,} {classic:>10,} {electric:>10,} {pct:>9.1f}%")

    print_header("Bike Type by User Type (Latest Month)")
    latest = data["monthly_detail"][-1]["month"] if data["monthly_detail"] else "?"
    print(f"  Month: {latest}")
    for row in data["monthly_detail"]:
        if row["month"] == latest:
            print(f"  {row['member_casual']:<8} {row['rideable_type']:<15} {row['rides']:>10,}")


def show_user_patterns():
    data = load_json("user_patterns.json")
    if not data:
        return

    print_header("Members vs Casual Riders")
    print(f"  {'Type':<10} {'Rides':>14} {'Avg Duration (min)':>20}")
    print(f"  {'-'*10} {'-'*14} {'-'*20}")
    for utype, info in data["overall"].items():
        print(f"  {utype:<10} {info['total_rides']:>14,} {info['avg_duration_min']:>20.1f}")

    print_header("Rides by Day of Week")
    days_member = {d["day"]: d["rides"] for d in data["by_day"]["member"]}
    days_casual = {d["day"]: d["rides"] for d in data["by_day"]["casual"]}
    print(f"  {'Day':<10} {'Member':>12} {'Casual':>12}")
    print(f"  {'-'*10} {'-'*12} {'-'*12}")
    for day in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        print(f"  {day:<10} {days_member.get(day, 0):>12,} {days_casual.get(day, 0):>12,}")

    print_header("Weekend vs Weekday")
    for utype in ["member", "casual"]:
        wd = data["by_weekend"][utype]["weekday"]
        we = data["by_weekend"][utype]["weekend"]
        print(f"  {utype}: weekday={wd['rides']:,} rides ({wd['avg_duration_min']:.0f} min avg)"
              f"  weekend={we['rides']:,} rides ({we['avg_duration_min']:.0f} min avg)")


def show_peak_hours():
    data = load_json("peak_hours.json")
    if not data:
        return

    print_header("Peak Hours — Weekday vs Weekend")
    print(f"  {'Hour':<8} {'WD Rides':>10} {'WE Rides':>10}")
    print(f"  {'-'*8} {'-'*10} {'-'*10}")
    wd = {h["hour"]: h["rides"] for h in data["simple"]["weekday"]}
    we = {h["hour"]: h["rides"] for h in data["simple"]["weekend"]}
    for h in range(24):
        print(f"  {h:02d}:00   {wd.get(h, 0):>10,} {we.get(h, 0):>10,}")

    print_header("Peak Hours by Season — Weekdays")
    for season in ["Spring", "Summer", "Fall", "Winter"]:
        if season in data["by_season"]["weekday"]:
            entries = data["by_season"]["weekday"][season]
            peak = max(entries, key=lambda x: x["rides"])
            total = sum(e["rides"] for e in entries)
            print(f"  {season:<10} peak: {peak['hour']:02d}:00 "
                  f"({peak['rides']:>10,} rides)  total: {total:>12,}")


def show_yoy_growth():
    data = load_json("yoy_growth.json")
    if not data:
        return

    print_header("Year-over-Year Ridership (2023 vs 2024)")
    print(f"  {'Month':<8} {'2023':>12} {'2024':>12} {'Change':>10}")
    print(f"  {'-'*8} {'-'*12} {'-'*12} {'-'*10}")
    for row in data["monthly"]:
        ch = f"{row['change_pct']:+.1f}%" if row["change_pct"] is not None else "N/A"
        print(f"  {row['month_name']:<8} {row['rides_2023']:>12,} {row['rides_2024']:>12,} {ch:>10}")

    yt = data["yearly_totals"]
    print(f"\n  2023 total: {yt['2023']:,}")
    print(f"  2024 total: {yt['2024']:,}")
    print(f"  Growth:     {yt['growth_pct']}%")


def show_trip_durations():
    data = load_json("trip_durations.json")
    if not data:
        return

    print_header("Trip Duration by Bike Type")
    print(f"  {'Bike Type':<15} {'Median (min)':>15} {'P95 (min)':>15} {'Rides':>12}")
    print(f"  {'-'*15} {'-'*15} {'-'*15} {'-'*12}")
    for row in data["by_bike_type"]:
        print(f"  {row['rideable_type']:<15} {row['median_min']:>15.1f} "
              f"{row['p95_min']:>15.1f} {row['rides']:>12,}")

    print_header("Trip Duration by Bike + User Type")
    print(f"  {'Type':<25} {'Median (min)':>15} {'P95 (min)':>15}")
    print(f"  {'-'*25} {'-'*15} {'-'*15}")
    for row in data["by_bike_and_user"]:
        label = f"{row['rideable_type']} / {row['member_casual']}"
        print(f"  {label:<25} {row['median_min']:>15.1f} {row['p95_min']:>15.1f}")


def show_top_routes():
    data = load_json("top_routes.json")
    if not data:
        return

    print_header("Top 15 One-Way Routes")
    print(f"  {'#':<4} {'From':<30} -> {'To':<30} {'Rides':>10}")
    print(f"  {'-'*4} {'-'*30} -> {'-'*30} {'-'*10}")
    for i, r in enumerate(data["top_oneway"][:15], 1):
        print(f"  {i:<4} {r['start_station'][:30]:<30} -> {r['end_station'][:30]:<30} {r['rides']:>10,}")

    print_header("Top 10 Circular Routes (A -> A)")
    for i, r in enumerate(data["top_circular"][:10], 1):
        print(f"  {i:<4} {r['station'][:50]:<50} {r['rides']:>10,}")

    print_header("Most Asymmetric Routes (one direction dominates)")
    for i, r in enumerate(data["asymmetric_routes"][:10], 1):
        print(f"  {r['route'][:55]:55} ratio={r['ratio']:.1f}:1")


def show_seasonal_impact():
    data = load_json("seasonal_impact.json")
    if not data:
        return

    print_header("Rides by Season")
    for season, rides in data["volume"].items():
        print(f"  {season:<10} {rides:>12,} rides")

    print_header("Avg Duration by Season")
    for season, info in data["duration"].items():
        print(f"  {season:<10} {info['avg_min']:>8.1f} min avg  "
              f"(median: {info['median_min']:.1f} min)")

    print_header("Bike Mix by Season")
    for season, bikes in data["bike_mix"].items():
        total = sum(bikes.values())
        elec = bikes.get("electric_bike", 0)
        print(f"  {season:<10} electric={elec/total*100:.1f}%  "
              f"(classic={bikes.get('classic_bike', 0):,}, electric={elec:,})")

    print_header("User Mix by Season")
    for season, users in data["user_mix"].items():
        total = sum(users.values())
        member = users.get("member", 0)
        print(f"  {season:<10} member={member/total*100:.1f}%  "
              f"(member={member:,}, casual={users.get('casual', 0):,})")


def show_circular_trips():
    data = load_json("circular_trips.json")
    if not data:
        return

    print_header("Circular Trips by User Type")
    for utype, info in data["by_user_type"].items():
        total = info["total_rides"]
        circ = info["circular_rides"]
        rate = info["circular_rate_pct"]
        print(f"  {utype:<10} {circ:>10,} / {total:>10,} = {rate:.1f}% circular")

    print_header("Duration Comparison (Circular vs One-Way)")
    for label, info in data["duration_comparison"].items():
        print(f"  {label:<25} median: {info['median_min']:.1f} min  "
              f"avg: {info['avg_min']:.1f} min")

    print_header("Top 15 Stations by Circular Trip Count")
    for i, s in enumerate(data["top_by_count"][:15], 1):
        print(f"  {i:2}. {s['station'][:45]:45} {s['circular_rides']:>8,} rides")

    print_header("Top 15 Stations by Circular Trip Rate")
    for i, s in enumerate(data["top_by_rate"][:15], 1):
        print(f"  {i:2}. {s['station'][:45]:45} {s['circular_rate_pct']:.1f}% circular "
              f"({s['circular_rides']:,} of {s['total_rides']:,})")


def show_recommendation():
    data = load_json("recommendations.json")
    if not data:
        return

    top5 = load_json("recommendations_top5.json")

    print_header("Station Recommendation")
    print("  Where should you start your ride to most likely find a bike?")
    print()

    try:
        hour_str = input("  Enter hour (0-23): ").strip()
        hour = int(hour_str)
        if hour < 0 or hour > 23:
            print("  Invalid hour. Must be 0-23.")
            return
    except (ValueError, EOFError):
        print("  Invalid input.")
        return

    user_type = input("  User type (member/casual): ").strip().lower()
    if user_type not in ("member", "casual"):
        print("  Invalid user type. Use 'member' or 'casual'.")
        return

    h = str(hour)
    if h not in data:
        print(f"  No data for hour {hour}.")
        return

    best = data[h].get(user_type)
    if not best:
        print(f"  No recommendation for {user_type} at hour {hour}.")
        return

    print()
    print(f"  Best station for a {user_type} at {hour:02d}:00:")
    print(f"    {best['station']}")
    print(f"    Net inflow score: {best['score']:+,}")
    if best["score"] > 0:
        print(f"    (More trips end here than start — bikes tend to accumulate)")
    else:
        print(f"    (More trips start here than end — bikes may be scarce)")

    # show top 5
    if top5 and h in top5 and user_type in top5[h]:
        print()
        print(f"  Top 5 alternatives:")
        for i, s in enumerate(top5[h][user_type], 1):
            print(f"    {i}. {s['station'][:50]:50} score={s['score']:+,}")


def show_menu():
    print()
    print("=" * 60)
    print("  NYC-Pedalytics — Citi Bike Data Explorer")
    print("=" * 60)
    print()
    print("  1. Popular stations (by month, start vs end)")
    print("  2. E-bike vs classic bike trends")
    print("  3. Member vs casual rider patterns")
    print("  4. Peak usage hours (weekday/weekend/season)")
    print("  5. Year-over-year ridership (2023 vs 2024)")
    print("  6. Trip duration distributions")
    print("  7. Top station-to-station routes")
    print("  8. Seasonal impact on ride patterns")
    print("  9. Circular trips (leisure/tourist behavior)")
    print("  10. Get a station recommendation")
    print("  0. Exit")
    print()


def main():
    handlers = {
        1: ("Popular stations", show_popular_stations),
        2: ("E-bike vs classic trends", show_bike_type_trends),
        3: ("Member vs casual patterns", show_user_patterns),
        4: ("Peak hours", show_peak_hours),
        5: ("Year-over-year growth", show_yoy_growth),
        6: ("Trip durations", show_trip_durations),
        7: ("Top routes", show_top_routes),
        8: ("Seasonal impact", show_seasonal_impact),
        9: ("Circular trips", show_circular_trips),
        10: ("Recommendation", show_recommendation),
    }

    while True:
        show_menu()
        try:
            choice = input("  Your choice (0-10): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Bye!")
            break

        if choice == "0":
            print("  Bye!")
            break

        try:
            idx = int(choice)
        except ValueError:
            print("  Please enter a number 0-10.")
            continue

        if idx in handlers:
            name, func = handlers[idx]
            print(f"\n  [Loading: {name}]")
            func()
            try:
                input("\n  Press Enter to return to menu...")
            except (EOFError, KeyboardInterrupt):
                print("\n  Bye!")
                break
        else:
            print("  Invalid choice. Pick 0-10.")


if __name__ == "__main__":
    main()
