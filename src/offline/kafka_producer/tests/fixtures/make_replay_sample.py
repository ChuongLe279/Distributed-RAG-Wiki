"""Sinh tests/fixtures/replay_sample.jsonl (nội dung tự viết, không lấy từ Wikipedia).

Chạy: python tests/fixtures/make_replay_sample.py
"""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

OUTPUT = Path(__file__).with_name("replay_sample.jsonl")

# (article_id, title, loại, nơi chốn, lĩnh vực, các chi tiết riêng)
TOPICS = [
    (1002, "Aldermoor Lighthouse", "lighthouse", "the northern coast of Verran", "maritime navigation",
     ["granite tower", "rotating lens", "fog signal", "keeper's cottage"]),
    (1003, "Brennick Valley Railway", "narrow-gauge railway", "the Brennick Valley", "rail transport",
     ["steam locomotives", "slate quarries", "timber viaducts", "passenger carriages"]),
    (1004, "Calloway Observatory", "astronomical observatory", "the Tessin highlands", "optical astronomy",
     ["reflecting telescope", "spectrograph", "variable stars", "photographic plates"]),
    (1005, "Dunmere Abbey", "ruined monastery", "the county of Halloway", "medieval architecture",
     ["cloister arcade", "chapter house", "illuminated manuscripts", "fish ponds"]),
    (1006, "Elowen Marsh", "coastal wetland", "the Sarn estuary", "wetland ecology",
     ["reed beds", "migratory waders", "tidal creeks", "salt-tolerant grasses"]),
    (1007, "Farrowgate Bridge", "suspension bridge", "the city of Ostrava Bay", "civil engineering",
     ["steel cables", "masonry towers", "tram lines", "wind bracing"]),
    (1008, "Greywater Brewery", "brewery", "the town of Pellham", "brewing",
     ["copper kettles", "oak barrels", "spring water", "barley malt"]),
    (1009, "Halvard Institute", "research institute", "the university district of Norrby", "materials science",
     ["ceramic composites", "electron microscopes", "thin films", "doctoral training"]),
    (1010, "Isenbrook Dam", "hydroelectric dam", "the Isen river gorge", "power generation",
     ["concrete arch", "turbine hall", "spillway gates", "reservoir levels"]),
    (1011, "Juniper Fold Cheese", "sheep's-milk cheese", "the upland farms of Carrow", "dairy farming",
     ["natural rind", "cave ageing", "raw milk", "seasonal grazing"]),
    (1012, "Kestrel Point Museum", "local history museum", "the harbour of Kestrel Point", "museum curation",
     ["whaling logbooks", "ship models", "oral histories", "textile collection"]),
    (1013, "Lowen Glass Works", "glass factory", "the village of Lowen", "glassmaking",
     ["blown crystal", "coal furnaces", "engraved goblets", "pattern books"]),
    (12000003, "Marrowfield Cricket Club", "cricket club", "the suburb of Marrowfield", "amateur sport",
     ["pavilion", "grass wicket", "junior teams", "league titles"]),
    (12000004, "Northcombe Tram Depot", "tram depot", "the district of Northcombe", "urban transit",
     ["inspection pits", "overhead wires", "double-deck trams", "traction motors"]),
    (25000007, "Orrin Peak", "mountain", "the Daleth range", "mountaineering",
     ["glacial cirque", "north ridge", "alpine meadows", "summit cairn"]),
    (25000008, "Pellham Free Library", "public library", "the centre of Pellham", "public education",
     ["reading room", "newspaper archive", "lending desk", "children's wing"]),
]


def prose(title: str, kind: str, place: str, field: str, details: list[str]) -> str:
    a, b, c, d = details
    return "\n\n".join([
        f"'''{title}''' is a {kind} located in {place}. It is regarded as one of the most notable examples of "
        f"{field} in the region, and it has been studied by local historians for several generations. The site is "
        f"best known for its {a} and its {b}, both of which survive in largely original condition and attract a "
        f"steady number of visitors every year.",
        f"== History ==\nConstruction began after a public subscription raised the necessary funds, and the first "
        f"phase of work was finished within six years. Early records describe the {c} in considerable detail, "
        f"noting that skilled workers were brought from neighbouring districts to complete it. During the following "
        f"decades the {kind} was enlarged twice, and the {d} were added as part of the second enlargement. A period "
        f"of decline followed when funding was withdrawn, but a preservation society later took over its care.",
        f"== Present day ==\nToday {title} is managed by a charitable trust that organises guided tours and "
        f"publishes an annual report on {field}. Volunteers maintain the {a} throughout the year, while "
        f"specialists inspect the {b} every spring. The trust has also digitised its archive, so researchers can "
        f"consult drawings of the {c} and photographs of the {d} without travelling to {place}.",
    ])


TARN_BASE = (
    "'''Tarn Hollow''' is a small glacial lake in the Wetherby uplands. It lies at the head of a steep valley and "
    "is fed by two streams that rise on the surrounding moorland. The lake is roughly oval in shape and is "
    "enclosed on three sides by cliffs of dark volcanic rock. Walkers usually reach it by a stone path that "
    "climbs from the hamlet of Low Garth, a journey of about two hours.\n\n"
    "The water is cold and clear throughout the year, and it supports a small population of brown trout. "
    "Naturalists have recorded several species of dragonfly along the northern shore, where a belt of sedge "
    "provides shelter from the wind. In winter the surface often freezes for several weeks, and the valley "
    "below can be cut off by snow."
)
TARN_ADDITION = (
    "\n\n== Geology ==\nThe basin was carved by ice during the last glacial period, and a ridge of moraine "
    "still dams the outflow. Geologists from the regional survey mapped the cliffs in detail and identified "
    "three separate lava flows, each separated by a thin layer of ash. Samples taken from the lake bed contain "
    "pollen that records how the surrounding vegetation changed after the ice retreated. Birch and juniper "
    "appeared first, followed by oak woodland, and finally by the open heath that covers the slopes today. "
    "The survey also measured the depth of the lake, which reaches nineteen metres near the western cliff."
)
TARN_FINAL = (
    "'''Tarn Hollow''' is a protected glacial lake in the Wetherby uplands, designated as a nature reserve. "
    "It lies at the head of a steep valley and is fed by two streams that rise on the surrounding moorland. "
    "Access is limited to a single marked path from the hamlet of Low Garth, and camping on the shore is not "
    "permitted. The reserve is managed by a county wildlife trust, which monitors the brown trout population "
    "and the dragonflies that breed along the northern shore.\n\n"
    "Wardens visit the lake every week between April and October. They record water temperature, repair the "
    "path after heavy rain, and advise walkers about nesting birds on the cliffs. A short annual report "
    "summarises these observations for the public."
)

RICH = """{{Infobox settlement
| name = Quillon Harbour
| population = 4,812
| country = Verran
| established = 1764
}}
[[File:Quillon harbour view.jpg|thumb|The inner basin at low tide]]
'''Quillon Harbour''' is a fishing town on the western shore of [[Verran]]. It grew around a natural inlet that offered shelter to small boats, and by the end of the eighteenth century it had become the main [[herring]] port of the district.<ref>Local Gazetteer, 1802, p. 44.</ref> The town is built on two terraces above the quay, and most of its older houses are faced with pale limestone.

== Economy ==
Fishing remains the largest employer, although tourism has grown steadily since the coastal road was improved.<ref name="report">{{cite report |title=District Economic Survey |year=1998}}</ref> A boatyard on the northern side of the inlet repairs wooden vessels and trains apprentices in traditional methods. Several small smokehouses still cure fish over oak chips and sell their produce at the weekly market.

{| class="wikitable"
|-
! Year !! Boats registered !! Catch (tonnes)
|-
| 1900 || 212 || 9,400
|-
| 1950 || 131 || 6,150
|-
| 2000 || 48 || 2,300
|}

== Landmarks ==
* The Customs House, built in 1790
* St Brannoc's Chapel
* The Old Net Loft
# First item of a numbered list
; Term
: Definition of the term

The harbour wall was rebuilt after a severe storm destroyed the original breakwater. Engineers raised the new wall by nearly two metres and added a curved parapet to deflect waves away from the moored boats. A small light at the end of the wall guides vessels through the narrow entrance at night.

[[Category:Fishing towns]]
[[Category:Populated places established in 1764]]
"""

DISAMBIGUATION = """'''Mercury Hollow''' may refer to:

* [[Mercury Hollow (village)]], a village in Halloway
* [[Mercury Hollow (album)]], a 1987 folk album
* [[Mercury Hollow railway station]], a closed station on the Brennick Valley Railway

{{disambiguation}}
"""

REDIRECT = "#REDIRECT [[Aldermoor Lighthouse]]\n\n{{R from alternative name}}\n"


def main() -> None:
    clock = datetime(2026, 1, 5, 8, 0, 0, tzinfo=timezone.utc)
    revision = 5000
    messages = []

    def add(article_id: int, title: str, text: str, event_type: str = "NEW") -> None:
        nonlocal clock, revision
        revision += 1
        clock += timedelta(minutes=7)
        messages.append({
            "article_id": article_id, "revision_id": revision, "title": title, "raw_wikitext": text,
            "event_type": event_type, "event_time": clock.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "replay",
        })

    add(1001, "Tarn Hollow", TARN_BASE)                                   # NEW
    for topic in TOPICS[:6]:
        add(topic[0], topic[1], prose(*topic[1:]))
    add(1001, "Tarn Hollow", TARN_BASE + TARN_ADDITION, "UPDATED")        # UPDATED lần 1: dài hơn
    add(1020, "Quillon Harbour", RICH)                                    # bảng, infobox, danh sách
    add(1021, "Mercury Hollow (disambiguation)", DISAMBIGUATION)          # trang định hướng
    for topic in TOPICS[6:11]:
        add(topic[0], topic[1], prose(*topic[1:]))
    add(1022, "Aldermoor Light", REDIRECT)                                # trang redirect
    add(1001, "Tarn Hollow", TARN_FINAL, "UPDATED")                       # UPDATED lần 2: ngắn hơn
    for topic in TOPICS[11:]:
        add(topic[0], topic[1], prose(*topic[1:]))

    with OUTPUT.open("w", encoding="utf-8", newline="\n") as handle:
        for message in messages:
            handle.write(json.dumps(message, ensure_ascii=False) + "\n")
    print(f"{len(messages)} message, {len({m['article_id'] for m in messages})} articles -> {OUTPUT}")


if __name__ == "__main__":
    main()
