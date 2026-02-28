"use client";

import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useRef, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import { AnalyticsSection } from "@/components/gameplay-stats/AnalyticsSection";
import { CardsSection } from "@/components/gameplay-stats/CardsSection";
import { CoachingSection } from "@/components/gameplay-stats/CoachingSection";
import { GameLogSection } from "@/components/gameplay-stats/GameLogSection";
import { SectionTabs } from "@/components/gameplay-stats/SectionTabs";
import { SummaryCards } from "@/components/gameplay-stats/SummaryCards";
import styles from "@/components/gameplay-stats/styles.module.css";
import type {
  CombinedArchetype,
  HitterSide,
  HitDataMap,
  HitDataStat,
  HitZoneKey,
  OutType,
  PitcherHand,
  PitchTypeRank,
  SectionTab,
  ShowAggregateStats,
  ShowCardPitchingStats,
  ShowCardStats,
  ShowGameLogItem,
  ShowGameSummary,
  ShowHitterSearchResult,
  ShowPitcherSearchResult,
  ShowProfile,
  ShowSkills,
  StrikeoutMapData,
  TimingType,
} from "@/components/gameplay-stats/types";
import Navbar from "@/components/navbar";
import { ApiError, apiGetAuth } from "@/lib/api";
import { toReadableAuthError } from "@/lib/auth-errors";
import { getFirebaseAuth } from "@/lib/firebase";
import { PROFILE_IMAGE_UPDATED_EVENT, resolveAvatarUrl } from "@/lib/profile-image";

const HIT_ZONE_KEYS: HitZoneKey[] = [
  "infield_left",
  "infield_right",
  "outfield_left",
  "outfield_center",
  "outfield_right",
  "homerun_left",
  "homerun_center",
  "homerun_right",
];

const HIT_STATS_KEYS: Array<keyof HitDataMap["stats"]> = [
  "sweet_spot_pct",
  "popup_rate",
  "flyball_rate",
  "groundball_rate",
  "gb_air_ratio",
  "pulled_air_rate",
  "oppo_air_rate",
  "perfect_perfect_pct",
  "extreme_contact_nopp_pct",
];

function aggregateHitMaps(maps: HitDataMap[], stat: HitDataStat): HitDataMap | null {
  if (maps.length === 0) {
    return null;
  }

  const zones: Record<HitZoneKey, number> = {
    infield_left: 0,
    infield_right: 0,
    outfield_left: 0,
    outfield_center: 0,
    outfield_right: 0,
    homerun_left: 0,
    homerun_center: 0,
    homerun_right: 0,
  };
  let total = 0;
  let pa = 0;

  const weightedSum: Partial<Record<keyof HitDataMap["stats"], number>> = {};
  const weightTotal: Partial<Record<keyof HitDataMap["stats"], number>> = {};

  maps.forEach((map) => {
    HIT_ZONE_KEYS.forEach((zoneKey) => {
      zones[zoneKey] += map.zones?.[zoneKey] ?? 0;
    });
    total += map.total ?? 0;
    pa += map.pa ?? 0;

    const weight = map.total && map.total > 0 ? map.total : 1;
    HIT_STATS_KEYS.forEach((statKey) => {
      const value = map.stats?.[statKey];
      if (value === null || value === undefined || Number.isNaN(value)) {
        return;
      }
      weightedSum[statKey] = (weightedSum[statKey] ?? 0) + value * weight;
      weightTotal[statKey] = (weightTotal[statKey] ?? 0) + weight;
    });
  });

  const stats = {
    sweet_spot_pct: (weightedSum.sweet_spot_pct ?? 0) / (weightTotal.sweet_spot_pct ?? 1),
    popup_rate: (weightedSum.popup_rate ?? 0) / (weightTotal.popup_rate ?? 1),
    flyball_rate: (weightedSum.flyball_rate ?? 0) / (weightTotal.flyball_rate ?? 1),
    groundball_rate: (weightedSum.groundball_rate ?? 0) / (weightTotal.groundball_rate ?? 1),
    gb_air_ratio: (weightedSum.gb_air_ratio ?? 0) / (weightTotal.gb_air_ratio ?? 1),
    pulled_air_rate: (weightedSum.pulled_air_rate ?? 0) / (weightTotal.pulled_air_rate ?? 1),
    oppo_air_rate: (weightedSum.oppo_air_rate ?? 0) / (weightTotal.oppo_air_rate ?? 1),
    perfect_perfect_pct: (weightedSum.perfect_perfect_pct ?? 0) / (weightTotal.perfect_perfect_pct ?? 1),
    extreme_contact_nopp_pct:
      (weightedSum.extreme_contact_nopp_pct ?? 0) / (weightTotal.extreme_contact_nopp_pct ?? 1),
  };

  return {
    zones,
    total,
    pa,
    stat,
    stats,
  };
}

export default function GameplayStatsPage() {
  const router = useRouter();
  const pitcherRequestRef = useRef(0);
  const hitterRequestRef = useRef(0);

  const [authReady, setAuthReady] = useState(false);
  const [firebaseUser, setFirebaseUser] = useState<User | null>(null);

  const [baseLoading, setBaseLoading] = useState(true);
  const [pageError, setPageError] = useState<string | null>(null);
  const [notLinked, setNotLinked] = useState(false);

  const [showProfile, setShowProfile] = useState<ShowProfile | null>(null);
  const [gameSummary, setGameSummary] = useState<ShowGameSummary | null>(null);
  const [skills, setSkills] = useState<ShowSkills | null>(null);
  const [battingArchetype, setBattingArchetype] = useState<CombinedArchetype["batting"] | null>(null);
  const [pitchingArchetype, setPitchingArchetype] = useState<CombinedArchetype["pitching"] | null>(null);

  const [profileAvatar, setProfileAvatar] = useState<string | null>(null);
  const [skillMode, setSkillMode] = useState<"Hitting" | "Pitching">("Hitting");
  const [activeTab, setActiveTab] = useState<SectionTab>("Analytics");

  const [strikeoutMode, setStrikeoutMode] = useState<"Hitting" | "Pitching">("Hitting");
  const [strikeoutMap, setStrikeoutMap] = useState<StrikeoutMapData | null>(null);
  const [strikeoutLoading, setStrikeoutLoading] = useState(false);
  const [strikeoutError, setStrikeoutError] = useState<string | null>(null);
  const [filterHitterSide, setFilterHitterSide] = useState<HitterSide>("all");
  const [filterPitcherHand, setFilterPitcherHand] = useState<PitcherHand>("all");
  const [filterPitchTypes, setFilterPitchTypes] = useState<string[]>([]);
  const [advancedFiltersOpen, setAdvancedFiltersOpen] = useState(false);
  const [advancedMinSpeed, setAdvancedMinSpeed] = useState("");
  const [advancedMaxSpeed, setAdvancedMaxSpeed] = useState("");
  const [advancedTiming, setAdvancedTiming] = useState<TimingType>("all");
  const [advancedOutType, setAdvancedOutType] = useState<OutType>("all");
  const [selectedPitcher, setSelectedPitcher] = useState<ShowPitcherSearchResult | null>(null);
  const [selectedHitter, setSelectedHitter] = useState<ShowHitterSearchResult | null>(null);
  const [pitcherSearchQuery, setPitcherSearchQuery] = useState("");
  const [hitterSearchQuery, setHitterSearchQuery] = useState("");
  const [pitcherSearchResults, setPitcherSearchResults] = useState<ShowPitcherSearchResult[]>([]);
  const [hitterSearchResults, setHitterSearchResults] = useState<ShowHitterSearchResult[]>([]);
  const [pitcherSearchLoading, setPitcherSearchLoading] = useState(false);
  const [hitterSearchLoading, setHitterSearchLoading] = useState(false);
  const [pitcherSearchError, setPitcherSearchError] = useState<string | null>(null);
  const [hitterSearchError, setHitterSearchError] = useState<string | null>(null);
  const [sprayChartMode, setSprayChartMode] = useState<"Hitting" | "Pitching">("Hitting");
  const [sprayChartStat, setSprayChartStat] = useState<HitDataStat>("count");
  const [sprayChartSelections, setSprayChartSelections] = useState<HitZoneKey[]>([]);
  const [sprayChartData, setSprayChartData] = useState<HitDataMap | null>(null);
  const [sprayChartLoading, setSprayChartLoading] = useState(false);
  const [sprayChartError, setSprayChartError] = useState<string | null>(null);
  const [sprayChartSelectionData, setSprayChartSelectionData] = useState<HitDataMap | null>(null);
  const [sprayChartSelectionLoading, setSprayChartSelectionLoading] = useState(false);
  const [sprayChartSelectionError, setSprayChartSelectionError] = useState<string | null>(null);

  const [statsMode, setStatsMode] = useState<"Hitting" | "Pitching">("Hitting");
  const [aggregateStats, setAggregateStats] = useState<ShowAggregateStats | null>(null);
  const [aggregateLoading, setAggregateLoading] = useState(false);
  const [aggregateError, setAggregateError] = useState<string | null>(null);

  const [gameLog, setGameLog] = useState<ShowGameLogItem[]>([]);
  const [gameLogLoading, setGameLogLoading] = useState(false);
  const [gameLogError, setGameLogError] = useState<string | null>(null);

  const [hittingCards, setHittingCards] = useState<ShowCardStats[]>([]);
  const [hittingCardsLoading, setHittingCardsLoading] = useState(false);
  const [hittingCardsError, setHittingCardsError] = useState<string | null>(null);

  const [pitchingCards, setPitchingCards] = useState<ShowCardPitchingStats[]>([]);
  const [pitchingCardsLoading, setPitchingCardsLoading] = useState(false);
  const [pitchingCardsError, setPitchingCardsError] = useState<string | null>(null);

  const [hitData, setHitData] = useState<HitDataMap | null>(null);
  const [coachingStrikeoutMap, setCoachingStrikeoutMap] = useState<StrikeoutMapData | null>(null);
  const [coachingLoading, setCoachingLoading] = useState(false);
  const [coachingError, setCoachingError] = useState<string | null>(null);

  const [pitchTypeRanks, setPitchTypeRanks] = useState<PitchTypeRank[]>([]);
  const [pitchTypeRanksLoading, setPitchTypeRanksLoading] = useState(false);
  const [pitchTypeRanksError, setPitchTypeRanksError] = useState<string | null>(null);

  useEffect(() => {
    let unsubscribe: (() => void) | null = null;
    try {
      const auth = getFirebaseAuth();
      unsubscribe = onAuthStateChanged(auth, (user) => {
        setFirebaseUser(user);
        setAuthReady(true);
        if (!user) {
          router.replace("/signin");
        }
      });
    } catch (error: unknown) {
      setPageError(toReadableAuthError(error, "Firebase is not configured for web auth."));
      setAuthReady(true);
      setFirebaseUser(null);
    }

    return () => {
      if (unsubscribe) {
        unsubscribe();
      }
    };
  }, [router]);

  useEffect(() => {
    if (!firebaseUser) {
      setProfileAvatar(null);
      return;
    }

    let active = true;

    const loadAvatar = async (bustCache = false) => {
      const path = `users/${firebaseUser.uid}/profile.jpg`;
      const url = await resolveAvatarUrl(path, { bustCache });
      if (active) {
        setProfileAvatar(url);
      }
    };

    void loadAvatar();

    const onProfileImageUpdated = () => {
      void loadAvatar(true);
    };

    window.addEventListener(PROFILE_IMAGE_UPDATED_EVENT, onProfileImageUpdated);
    return () => {
      active = false;
      window.removeEventListener(PROFILE_IMAGE_UPDATED_EVENT, onProfileImageUpdated);
    };
  }, [firebaseUser?.uid]);

  useEffect(() => {
    if (!authReady || !firebaseUser) {
      if (authReady) {
        setBaseLoading(false);
      }
      return;
    }

    let active = true;
    setBaseLoading(true);
    setPageError(null);
    setNotLinked(false);

    void (async () => {
      try {
        const profileData = await apiGetAuth<ShowProfile>("/users/me/show");
        if (!active) {
          return;
        }

        const [summaryResult, skillsResult, archetypeResult] = await Promise.allSettled([
          apiGetAuth<ShowGameSummary>("/users/me/show/summary"),
          apiGetAuth<ShowSkills>("/users/me/show/skills"),
          apiGetAuth<CombinedArchetype>("/users/me/show/archetype/combined"),
        ]);

        const summaryData = summaryResult.status === "fulfilled" ? summaryResult.value : null;
        const skillsData = skillsResult.status === "fulfilled" ? skillsResult.value : null;
        const archetypeData = archetypeResult.status === "fulfilled" ? archetypeResult.value : null;

        setShowProfile(profileData);
        setGameSummary(summaryData);
        setSkills(skillsData);
        setBattingArchetype(archetypeData?.batting ?? null);
        setPitchingArchetype(archetypeData?.pitching ?? null);
      } catch (error: unknown) {
        if (!active) {
          return;
        }

        if (error instanceof ApiError && error.status === 404) {
          setNotLinked(true);
          setShowProfile(null);
          setGameSummary(null);
          setSkills(null);
          setBattingArchetype(null);
          setPitchingArchetype(null);
        } else {
          setPageError(toReadableAuthError(error, "Unable to load gameplay stats"));
        }
      } finally {
        if (active) {
          setBaseLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [authReady, firebaseUser?.uid]);

  const linkedReady = !baseLoading && !pageError && !notLinked;
  const pitchTypeMenuOptions = strikeoutMap?.pitch_type_options ?? [];
  const hasAdvancedFilters =
    filterPitchTypes.length > 0 ||
    advancedMinSpeed !== "" ||
    advancedMaxSpeed !== "" ||
    advancedTiming !== "all" ||
    advancedOutType !== "all";

  const clampSpeedInput = (value: string): string => {
    const digits = value.replace(/[^0-9]/g, "");
    if (!digits) {
      return "";
    }
    const next = Math.max(0, Math.min(99, Number.parseInt(digits, 10)));
    return Number.isNaN(next) ? "" : String(next);
  };

  const togglePitchType = (pitchType: string) => {
    if (pitchType === "all") {
      setFilterPitchTypes([]);
      return;
    }
    setFilterPitchTypes((prev) =>
      prev.includes(pitchType) ? prev.filter((item) => item !== pitchType) : [...prev, pitchType],
    );
  };

  const handleResetStrikeoutFilters = () => {
    setFilterHitterSide("all");
    setFilterPitcherHand("all");
    setFilterPitchTypes([]);
    setAdvancedFiltersOpen(false);
    setAdvancedMinSpeed("");
    setAdvancedMaxSpeed("");
    setAdvancedTiming("all");
    setAdvancedOutType("all");
    setSelectedPitcher(null);
    setSelectedHitter(null);
    setPitcherSearchQuery("");
    setHitterSearchQuery("");
    setPitcherSearchResults([]);
    setHitterSearchResults([]);
    setPitcherSearchError(null);
    setHitterSearchError(null);
  };

  useEffect(() => {
    if (pitchTypeMenuOptions.length === 0) {
      setFilterPitchTypes([]);
      return;
    }
    setFilterPitchTypes((prev) => prev.filter((item) => pitchTypeMenuOptions.includes(item)));
  }, [pitchTypeMenuOptions.join("|")]);

  useEffect(() => {
    if (!linkedReady) {
      return;
    }
    const trimmed = pitcherSearchQuery.trim();
    if (!trimmed) {
      setPitcherSearchResults([]);
      setPitcherSearchLoading(false);
      setPitcherSearchError(null);
      return;
    }

    const handle = setTimeout(async () => {
      const requestId = pitcherRequestRef.current + 1;
      pitcherRequestRef.current = requestId;
      setPitcherSearchLoading(true);
      setPitcherSearchError(null);

      try {
        const params = new URLSearchParams({ q: trimmed, limit: "12" });
        const data = await apiGetAuth<ShowPitcherSearchResult[]>(`/users/me/show/pitchers?${params.toString()}`);
        if (pitcherRequestRef.current !== requestId) {
          return;
        }
        setPitcherSearchResults(Array.isArray(data) ? data : []);
      } catch (error: unknown) {
        if (pitcherRequestRef.current !== requestId) {
          return;
        }
        setPitcherSearchError(toReadableAuthError(error, "Pitcher search failed"));
        setPitcherSearchResults([]);
      } finally {
        if (pitcherRequestRef.current === requestId) {
          setPitcherSearchLoading(false);
        }
      }
    }, 250);

    return () => clearTimeout(handle);
  }, [linkedReady, pitcherSearchQuery]);

  useEffect(() => {
    if (!linkedReady) {
      return;
    }
    const trimmed = hitterSearchQuery.trim();
    if (!trimmed) {
      setHitterSearchResults([]);
      setHitterSearchLoading(false);
      setHitterSearchError(null);
      return;
    }

    const handle = setTimeout(async () => {
      const requestId = hitterRequestRef.current + 1;
      hitterRequestRef.current = requestId;
      setHitterSearchLoading(true);
      setHitterSearchError(null);

      try {
        const params = new URLSearchParams({ q: trimmed, limit: "12", view: strikeoutMode.toLowerCase() });
        const data = await apiGetAuth<ShowHitterSearchResult[]>(`/users/me/show/hitters?${params.toString()}`);
        if (hitterRequestRef.current !== requestId) {
          return;
        }
        setHitterSearchResults(Array.isArray(data) ? data : []);
      } catch (error: unknown) {
        if (hitterRequestRef.current !== requestId) {
          return;
        }
        setHitterSearchError(toReadableAuthError(error, "Hitter search failed"));
        setHitterSearchResults([]);
      } finally {
        if (hitterRequestRef.current === requestId) {
          setHitterSearchLoading(false);
        }
      }
    }, 250);

    return () => clearTimeout(handle);
  }, [linkedReady, hitterSearchQuery, strikeoutMode]);

  useEffect(() => {
    if (!linkedReady) {
      return;
    }

    let active = true;
    setStrikeoutLoading(true);
    setStrikeoutError(null);

    void (async () => {
      try {
        const params = new URLSearchParams();
        params.set("view", strikeoutMode.toLowerCase());

        if (filterHitterSide !== "all") {
          params.set("hitter_side", filterHitterSide === "left" ? "L" : "R");
        }
        if (filterPitcherHand !== "all") {
          params.set("pitcher_hand", filterPitcherHand === "left" ? "L" : "R");
        }
        if (selectedPitcher?.mlb_id) {
          params.set("pitcher_mlb_id", String(selectedPitcher.mlb_id));
        }
        if (selectedHitter?.mlb_id) {
          params.set("hitter_mlb_id", String(selectedHitter.mlb_id));
        }
        if (filterPitchTypes.length > 0) {
          params.set(
            "pitch_types",
            filterPitchTypes.map((item) => item.toLowerCase()).join(","),
          );
        }
        if (advancedMinSpeed) {
          params.set("min_speed", advancedMinSpeed);
        }
        if (advancedMaxSpeed) {
          params.set("max_speed", advancedMaxSpeed);
        }
        if (advancedTiming !== "all") {
          params.set("timing", advancedTiming);
        }
        if (advancedOutType !== "all") {
          params.set("out_type", advancedOutType);
        }

        const path = `/users/me/show/strikeout-map?${params.toString()}`;
        const data = await apiGetAuth<StrikeoutMapData>(path);
        if (active) {
          setStrikeoutMap(data);
        }
      } catch (error: unknown) {
        if (!active) {
          return;
        }
        if (error instanceof ApiError && error.status === 404) {
          setStrikeoutMap(null);
        } else {
          setStrikeoutError(toReadableAuthError(error, "Unable to load strikeout map"));
        }
      } finally {
        if (active) {
          setStrikeoutLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [
    linkedReady,
    strikeoutMode,
    filterHitterSide,
    filterPitcherHand,
    filterPitchTypes.join("|"),
    advancedMinSpeed,
    advancedMaxSpeed,
    advancedTiming,
    advancedOutType,
    selectedPitcher?.mlb_id,
    selectedHitter?.mlb_id,
  ]);

  useEffect(() => {
    if (!linkedReady) {
      return;
    }

    let active = true;
    setSprayChartLoading(true);
    setSprayChartError(null);

    void (async () => {
      try {
        const params = new URLSearchParams();
        params.set("view", sprayChartMode.toLowerCase());
        params.set("stat", sprayChartStat);

        if (filterHitterSide !== "all") {
          params.set("hitter_side", filterHitterSide === "left" ? "L" : "R");
        }
        if (filterPitcherHand !== "all") {
          params.set("pitcher_hand", filterPitcherHand === "left" ? "L" : "R");
        }
        if (selectedPitcher?.mlb_id) {
          params.set("pitcher_mlb_id", String(selectedPitcher.mlb_id));
        }
        if (selectedHitter?.mlb_id) {
          params.set("hitter_mlb_id", String(selectedHitter.mlb_id));
        }

        const data = await apiGetAuth<HitDataMap>(`/users/me/show/hit-map?${params.toString()}`);
        if (active) {
          setSprayChartData(data);
        }
      } catch (error: unknown) {
        if (!active) {
          return;
        }
        if (error instanceof ApiError && error.status === 404) {
          setSprayChartData(null);
        } else {
          setSprayChartError(toReadableAuthError(error, "Unable to load spray chart"));
        }
      } finally {
        if (active) {
          setSprayChartLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [
    linkedReady,
    sprayChartMode,
    sprayChartStat,
    filterHitterSide,
    filterPitcherHand,
    selectedPitcher?.mlb_id,
    selectedHitter?.mlb_id,
  ]);

  useEffect(() => {
    if (!linkedReady) {
      return;
    }
    if (sprayChartSelections.length === 0) {
      setSprayChartSelectionData(null);
      setSprayChartSelectionError(null);
      setSprayChartSelectionLoading(false);
      return;
    }

    let active = true;
    setSprayChartSelectionLoading(true);
    setSprayChartSelectionError(null);

    void (async () => {
      try {
        const responses = await Promise.all(
          sprayChartSelections.map(async (zone) => {
            const params = new URLSearchParams();
            params.set("view", sprayChartMode.toLowerCase());
            params.set("stat", sprayChartStat);
            if (filterHitterSide !== "all") {
              params.set("hitter_side", filterHitterSide === "left" ? "L" : "R");
            }
            if (filterPitcherHand !== "all") {
              params.set("pitcher_hand", filterPitcherHand === "left" ? "L" : "R");
            }
            if (selectedPitcher?.mlb_id) {
              params.set("pitcher_mlb_id", String(selectedPitcher.mlb_id));
            }
            if (selectedHitter?.mlb_id) {
              params.set("hitter_mlb_id", String(selectedHitter.mlb_id));
            }
            params.set("focus_zone", zone);
            return apiGetAuth<HitDataMap>(`/users/me/show/hit-map?${params.toString()}`);
          }),
        );

        if (!active) {
          return;
        }
        setSprayChartSelectionData(aggregateHitMaps(responses, sprayChartStat));
      } catch (error: unknown) {
        if (!active) {
          return;
        }
        setSprayChartSelectionData(null);
        setSprayChartSelectionError(toReadableAuthError(error, "Unable to load filtered spray chart"));
      } finally {
        if (active) {
          setSprayChartSelectionLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [
    linkedReady,
    sprayChartSelections.join("|"),
    sprayChartMode,
    sprayChartStat,
    filterHitterSide,
    filterPitcherHand,
    selectedPitcher?.mlb_id,
    selectedHitter?.mlb_id,
  ]);

  useEffect(() => {
    if (!linkedReady) {
      return;
    }

    let active = true;
    setAggregateLoading(true);
    setAggregateError(null);

    void (async () => {
      try {
        const path = `/users/me/show/stats?view=${statsMode.toLowerCase()}`;
        const data = await apiGetAuth<ShowAggregateStats>(path);
        if (active) {
          setAggregateStats(data);
        }
      } catch (error: unknown) {
        if (!active) {
          return;
        }
        if (error instanceof ApiError && error.status === 404) {
          setAggregateStats(null);
        } else {
          setAggregateError(toReadableAuthError(error, "Unable to load aggregate stats"));
        }
      } finally {
        if (active) {
          setAggregateLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [linkedReady, statsMode]);

  useEffect(() => {
    if (!linkedReady) {
      return;
    }

    let active = true;
    setGameLogLoading(true);
    setGameLogError(null);

    void (async () => {
      try {
        const data = await apiGetAuth<ShowGameLogItem[]>("/users/me/show/game-log?limit=200");
        if (active) {
          setGameLog(Array.isArray(data) ? data : []);
        }
      } catch (error: unknown) {
        if (!active) {
          return;
        }
        if (error instanceof ApiError && error.status === 404) {
          setGameLog([]);
        } else {
          setGameLogError(toReadableAuthError(error, "Unable to load game log"));
        }
      } finally {
        if (active) {
          setGameLogLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [linkedReady]);

  useEffect(() => {
    if (!linkedReady) {
      return;
    }

    let active = true;
    setHittingCardsLoading(true);
    setHittingCardsError(null);

    void (async () => {
      try {
        const data = await apiGetAuth<ShowCardStats[]>("/users/me/show/cards");
        if (active) {
          setHittingCards(Array.isArray(data) ? data : []);
        }
      } catch (error: unknown) {
        if (!active) {
          return;
        }
        if (error instanceof ApiError && error.status === 404) {
          setHittingCards([]);
        } else {
          setHittingCardsError(toReadableAuthError(error, "Unable to load hitting cards"));
        }
      } finally {
        if (active) {
          setHittingCardsLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [linkedReady]);

  useEffect(() => {
    if (!linkedReady) {
      return;
    }

    let active = true;
    setPitchingCardsLoading(true);
    setPitchingCardsError(null);

    void (async () => {
      try {
        const data = await apiGetAuth<ShowCardPitchingStats[]>("/users/me/show/cards/pitching");
        if (active) {
          setPitchingCards(Array.isArray(data) ? data : []);
        }
      } catch (error: unknown) {
        if (!active) {
          return;
        }
        if (error instanceof ApiError && error.status === 404) {
          setPitchingCards([]);
        } else {
          setPitchingCardsError(toReadableAuthError(error, "Unable to load pitching cards"));
        }
      } finally {
        if (active) {
          setPitchingCardsLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [linkedReady]);

  useEffect(() => {
    if (!linkedReady) {
      return;
    }

    let active = true;
    setCoachingLoading(true);
    setCoachingError(null);
    setPitchTypeRanksLoading(true);
    setPitchTypeRanksError(null);

    void (async () => {
      try {
        const [hitDataResult, baseStrikeoutResult] = await Promise.allSettled([
          apiGetAuth<HitDataMap>("/users/me/show/hit-map?view=hitting&stat=count"),
          apiGetAuth<StrikeoutMapData>("/users/me/show/strikeout-map?view=hitting"),
        ]);

        if (!active) {
          return;
        }

        if (hitDataResult.status === "fulfilled") {
          setHitData(hitDataResult.value);
        } else {
          setHitData(null);
        }

        let baseStrikeoutMap: StrikeoutMapData | null = null;
        if (baseStrikeoutResult.status === "fulfilled") {
          baseStrikeoutMap = baseStrikeoutResult.value;
          setCoachingStrikeoutMap(baseStrikeoutMap);
        } else {
          setCoachingStrikeoutMap(null);
        }

        if (!baseStrikeoutMap || !Array.isArray(baseStrikeoutMap.pitch_type_options) || baseStrikeoutMap.pitch_type_options.length === 0) {
          setPitchTypeRanks([]);
          return;
        }

        const rankResults = await Promise.all(
          baseStrikeoutMap.pitch_type_options.map(async (pitchType) => {
            const params = new URLSearchParams();
            params.set("view", "hitting");
            params.set("pitch_types", pitchType.toLowerCase());
            const data = await apiGetAuth<StrikeoutMapData>(`/users/me/show/strikeout-map?${params.toString()}`);
            return {
              pitchType,
              kPct: data?.stats?.k_pct ?? null,
            } as PitchTypeRank;
          }),
        );

        if (active) {
          setPitchTypeRanks(rankResults);
        }
      } catch (error: unknown) {
        if (!active) {
          return;
        }
        setCoachingError(toReadableAuthError(error, "Unable to load coaching data"));
        setPitchTypeRanksError(toReadableAuthError(error, "Unable to load pitch rankings"));
      } finally {
        if (active) {
          setCoachingLoading(false);
          setPitchTypeRanksLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [linkedReady]);

  return (
    <main className={styles.page}>
      <Navbar />
      <FloatingShieldsBackground />
      <div className={styles.texture} />

      <div className={styles.content}>
        <header className={styles.header}>
          <h1>Gameplay Engine</h1>
          <p>Data may be up to 12 hours behind</p>
        </header>

        <section className={styles.profileTab}>
          <Image
            src={profileAvatar || "/images/default_profile.png"}
            alt="Profile"
            width={32}
            height={32}
            className={styles.profileAvatar}
            unoptimized={Boolean(profileAvatar)}
          />
          <div className={styles.profileText}>
            <span>MLB The Show</span>
            <strong>{showProfile?.username ?? (baseLoading ? "Loading..." : "Not linked")}</strong>
          </div>
        </section>

        {baseLoading ? (
          <section className={styles.statusCard}>
            <p>Loading gameplay profile...</p>
          </section>
        ) : null}

        {pageError ? (
          <section className={styles.statusCard}>
            <p className={styles.error}>{pageError}</p>
          </section>
        ) : null}

        {notLinked ? (
          <section className={styles.statusCard}>
            <p>Link your MLB The Show account first.</p>
            <Link href="/account" className={styles.linkButton}>
              Open Account to Link
            </Link>
          </section>
        ) : null}

        {linkedReady ? (
          <>
            <SummaryCards
              gameSummary={gameSummary}
              skills={skills}
              battingArchetype={battingArchetype}
              pitchingArchetype={pitchingArchetype}
              skillMode={skillMode}
              onChangeSkillMode={setSkillMode}
            />

            <SectionTabs activeTab={activeTab} onChange={setActiveTab} />

            {activeTab === "Analytics" ? (
              <AnalyticsSection
                strikeoutMode={strikeoutMode}
                onChangeStrikeoutMode={setStrikeoutMode}
                strikeoutMap={strikeoutMap}
                strikeoutLoading={strikeoutLoading}
                strikeoutError={strikeoutError}
                filterHitterSide={filterHitterSide}
                onChangeFilterHitterSide={(value) => {
                  setFilterHitterSide(value);
                  setSelectedHitter(null);
                }}
                filterPitcherHand={filterPitcherHand}
                onChangeFilterPitcherHand={(value) => {
                  setFilterPitcherHand(value);
                  setSelectedPitcher(null);
                }}
                selectedPitcher={selectedPitcher}
                selectedHitter={selectedHitter}
                pitcherSearchQuery={pitcherSearchQuery}
                onChangePitcherSearchQuery={setPitcherSearchQuery}
                hitterSearchQuery={hitterSearchQuery}
                onChangeHitterSearchQuery={setHitterSearchQuery}
                pitcherSearchResults={pitcherSearchResults}
                hitterSearchResults={hitterSearchResults}
                pitcherSearchLoading={pitcherSearchLoading}
                hitterSearchLoading={hitterSearchLoading}
                pitcherSearchError={pitcherSearchError}
                hitterSearchError={hitterSearchError}
                onSelectPitcher={(pitcher) => {
                  setSelectedPitcher(pitcher);
                  setPitcherSearchQuery("");
                  setPitcherSearchResults([]);
                }}
                onSelectHitter={(hitter) => {
                  setSelectedHitter(hitter);
                  setHitterSearchQuery("");
                  setHitterSearchResults([]);
                }}
                pitchTypeOptions={pitchTypeMenuOptions}
                selectedPitchTypes={filterPitchTypes}
                onTogglePitchType={togglePitchType}
                minSpeed={advancedMinSpeed}
                onChangeMinSpeed={(value) => setAdvancedMinSpeed(clampSpeedInput(value))}
                maxSpeed={advancedMaxSpeed}
                onChangeMaxSpeed={(value) => setAdvancedMaxSpeed(clampSpeedInput(value))}
                timing={advancedTiming}
                onChangeTiming={setAdvancedTiming}
                outType={advancedOutType}
                onChangeOutType={setAdvancedOutType}
                advancedOpen={advancedFiltersOpen}
                onToggleAdvanced={() => setAdvancedFiltersOpen((prev) => !prev)}
                onCloseAdvanced={() => setAdvancedFiltersOpen(false)}
                onResetFilters={handleResetStrikeoutFilters}
                hasAdvancedFilters={hasAdvancedFilters}
                statsMode={statsMode}
                onChangeStatsMode={setStatsMode}
                aggregateStats={aggregateStats}
                aggregateLoading={aggregateLoading}
                aggregateError={aggregateError}
                sprayChartData={sprayChartData}
                sprayChartLoading={sprayChartLoading}
                sprayChartError={sprayChartError || sprayChartSelectionError}
                sprayChartSnapshotData={sprayChartSelections.length > 0 ? sprayChartSelectionData : sprayChartData}
                sprayChartSnapshotLoading={sprayChartLoading || (sprayChartSelections.length > 0 && sprayChartSelectionLoading)}
                sprayChartMode={sprayChartMode}
                onChangeSprayChartMode={(mode) => {
                  setSprayChartMode(mode);
                }}
                sprayChartStat={sprayChartStat}
                onChangeSprayChartStat={(stat) => {
                  setSprayChartStat(stat);
                }}
                sprayChartSelections={sprayChartSelections}
                onChangeSprayChartSelection={(zone) => {
                  setSprayChartSelections((prev) => (prev.includes(zone) ? prev.filter((item) => item !== zone) : [...prev, zone]));
                }}
                onClearSprayChartSelections={() => setSprayChartSelections([])}
              />
            ) : null}

            {activeTab === "Game Log" ? (
              <GameLogSection
                games={gameLog}
                username={showProfile?.username ?? null}
                loading={gameLogLoading}
                error={gameLogError}
              />
            ) : null}

            {activeTab === "Cards" ? (
              <CardsSection
                hittingCards={hittingCards}
                pitchingCards={pitchingCards}
                loadingHitting={hittingCardsLoading}
                loadingPitching={pitchingCardsLoading}
                errorHitting={hittingCardsError}
                errorPitching={pitchingCardsError}
              />
            ) : null}

            {activeTab === "Coaching" ? (
              <CoachingSection
                hitData={hitData}
                strikeoutMapHitting={coachingStrikeoutMap}
                pitchTypeRanks={pitchTypeRanks}
                pitchTypeRanksLoading={pitchTypeRanksLoading}
                pitchTypeRanksError={pitchTypeRanksError}
                coachingLoading={coachingLoading}
                coachingError={coachingError}
              />
            ) : null}
          </>
        ) : null}
      </div>
    </main>
  );
}
