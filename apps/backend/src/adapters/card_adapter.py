from typing import List, Dict
from src.database.models import Card, Pitch
from src.adapters.base import BaseAdapter

class CardAdapter(BaseAdapter):
    
    def __init__(self, series_map: Dict, quirk_map: Dict, location_map: Dict):
        super().__init__()
        self.series_map = series_map
        self.quirk_map = quirk_map
        self.location_map = location_map

    def _card_id(self, year: int, source_uuid: str) -> str:
        return f"{year}:{source_uuid}"

    def run(self, data) -> List[Card]:
        cards = []
        for item in data:
            source_uuid = self._json_get(item, "source_uuid", "") or self._json_get(item, "uuid", "")
            year = self._json_get(item, "year", 0) or 0
            if not source_uuid or not year:
                continue
            
            card = Card()
            card.id = self._card_id(year, source_uuid)
            card.source_uuid = source_uuid
            card.year = self._json_get(item, "year", 0)
            card.name = self._json_get(item, "name", "Unknown")
            card.ovr = self._json_get(item, "ovr", 0) or 0
            card.type = self._json_get(item, "type", "")
            card.img = self._json_get(item, "img", "")
            card.baked_img = self._json_get(item, "baked_img", "")
            card.short_description = self._json_get(item, "short_description", "")
            card.rarity = self._json_get(item, "rarity", "")
            card.team = self._json_get(item, "team", "")
            card.team_short_name = self._json_get(item, "team_short_name", "")
            card.display_position = self._json_get(item, "display_position", "")
            card.display_secondary_positions = self._json_get(item, "display_secondary_positions", "")
            card.jersey_number = self._json_get(item, "jersey_number", 0) or 0
            card.age = self._json_get(item, "age", 0) or 0
            card.bat_hand = self._json_get(item, "bat_hand", "")
            card.throw_hand = self._json_get(item, "throw_hand", "")
            card.weight = self._json_get(item, "weight", "")
            card.height = self._json_get(item, "height", "")
            card.born = self._json_get(item, "born", "")
            card.is_hitter = self._json_get(item, "is_hitter", False)
            card.stamina = self._json_get(item, "stamina", 0) or 0
            card.pitching_clutch = self._json_get(item, "pitching_clutch", 0) or 0
            card.hits_per_bf = self._json_get(item, "hits_per_bf", 0) or 0
            card.k_per_bf = self._json_get(item, "k_per_bf", 0) or 0
            card.bb_per_bf = self._json_get(item, "bb_per_bf", 0) or 0
            card.hr_per_bf = self._json_get(item, "hr_per_bf", 0) or 0
            card.pitch_velocity = self._json_get(item, "pitch_velocity", 0) or 0
            card.pitch_control = self._json_get(item, "pitch_control", 0) or 0
            card.pitch_movement = self._json_get(item, "pitch_movement", 0) or 0
            card.contact_left = self._json_get(item, "contact_left", 0) or 0
            card.contact_right = self._json_get(item, "contact_right", 0) or 0
            card.power_left = self._json_get(item, "power_left", 0) or 0
            card.power_right = self._json_get(item, "power_right", 0) or 0
            card.plate_vision = self._json_get(item, "plate_vision", 0) or 0
            card.plate_discipline = self._json_get(item, "plate_discipline", 0) or 0
            card.batting_clutch = self._json_get(item, "batting_clutch", 0) or 0
            card.bunting_ability = self._json_get(item, "bunting_ability", 0) or 0
            card.drag_bunting_ability = self._json_get(item, "drag_bunting_ability", 0) or 0
            card.hitting_durability = self._json_get(item, "hitting_durability", 0) or 0
            card.fielding_durability = self._json_get(item, "fielding_durability", 0) or 0
            card.fielding_ability = self._json_get(item, "fielding_ability", 0) or 0
            card.arm_strength = self._json_get(item, "arm_strength", 0) or 0
            card.arm_accuracy = self._json_get(item, "arm_accuracy", 0) or 0
            card.reaction_time = self._json_get(item, "reaction_time", 0) or 0
            card.blocking = self._json_get(item, "blocking", 0) or 0
            card.speed = self._json_get(item, "speed", 0) or 0
            card.baserunning_ability = self._json_get(item, "baserunning_ability", 0) or 0
            card.baserunning_aggression = self._json_get(item, "baserunning_aggression", 0) or 0
            card.hit_rank_image = self._json_get(item, "hit_rank_image", "")
            card.fielding_rank_image = self._json_get(item, "fielding_rank_image", "")
            card.is_sellable = self._json_get(item, "is_sellable", False)
            card.has_augment = self._json_get(item, "has_augment", False)
            card.augment_text = self._json_get(item, "augment_text", "")
            card.augment_end_date = self._json_get(item, "augment_end_date", None) 
            card.has_matchup = self._json_get(item, "has_matchup", False)
            card.stars = self._json_get(item, "stars", "")
            card.trend = self._json_get(item, "trend", "")
            card.new_rank = self._json_get(item, "new_rank", 0) or 0
            card.has_rank_change = self._json_get(item, "has_rank_change", False)
            card.event = self._json_get(item, "event", False)
            card.set_name = self._json_get(item, "set_name", "")
            card.is_live_set = self._json_get(item, "is_live_set", False)
            card.ui_anim_index = self._json_get(item, "ui_anim_index", 0) or 0

            series_name = self._json_get(item, "series", "")
            if series_name and series_name in self.series_map:
                card.series_name = series_name
                card.series = self.series_map[series_name]
            
            item_quirks = self._json_get(item, "quirks", [])
            card_quirks = []
            for q in item_quirks:
                q_name = q.get("name")
                if q_name and q_name in self.quirk_map:
                    card_quirks.append(self.quirk_map[q_name])
            card.quirks = card_quirks

            item_locs = self._json_get(item, "locations", [])
            card_locs = []
            for l_name in item_locs:
                if l_name and l_name in self.location_map:
                    card_locs.append(self.location_map[l_name])
            card.locations = card_locs

            item_pitches = self._json_get(item, "pitches", [])
            pitch_objs = []
            for p in item_pitches:
                new_pitch = Pitch(
                    card_id=card.id,
                    name=p.get("name"),
                    speed=p.get("speed", 0),
                    control=p.get("control", 0),
                    movement=p.get("movement", 0)
                )
                pitch_objs.append(new_pitch)
            card.pitches = pitch_objs

            cards.append(card)

        return cards