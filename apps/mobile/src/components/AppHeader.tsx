import { useCallback, useEffect, useRef, useState } from "react";
import {
  Animated,
  Easing,
  Image,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  useWindowDimensions,
  View,
} from "react-native";
import { SafeAreaView, useSafeAreaInsets } from "react-native-safe-area-context";
import { usePathname, useRouter } from "expo-router";
import { FontAwesome5, Ionicons } from "@expo/vector-icons";

import { SearchMode, SearchResultsPanel } from "./SearchOverlay";
import { theme } from "../theme/colors";
import { useBackendProStatus } from "../lib/proStatus";

const LOGO_IMAGE = require("../../assets/images/logo.png");
const ACCENT = "#fbbf24";
const HEADER_HORIZONTAL_PADDING = 16;
const SEARCH_MIN_WIDTH = 280;
const SEARCH_MAX_WIDTH = 480;

export const AppHeader = () => {
  const router = useRouter();
  const pathname = usePathname();
  const insets = useSafeAreaInsets();
  const { width: windowWidth } = useWindowDimensions();
  const inputRef = useRef<TextInput>(null);
  const searchAnim = useRef(new Animated.Value(0)).current;
  const closingRef = useRef(false);
  const { isPro, refresh: refreshProStatus } = useBackendProStatus();

  const [searchOpen, setSearchOpen] = useState(false);
  const [renderSearch, setRenderSearch] = useState(false);
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState<SearchMode>("all");
  const searchTargetWidth = Math.min(
    SEARCH_MAX_WIDTH,
    Math.max(SEARCH_MIN_WIDTH, Math.round(windowWidth * 0.62))
  );

  useEffect(() => {
    if (!searchOpen) return;
    const handle = setTimeout(() => inputRef.current?.focus(), 40);
    return () => clearTimeout(handle);
  }, [searchOpen]);

  const runSearchAnim = useCallback((toValue: number, onDone?: () => void) => {
    searchAnim.stopAnimation();
    Animated.timing(searchAnim, {
      toValue,
      duration: toValue === 1 ? 220 : 180,
      easing: toValue === 1 ? Easing.out(Easing.cubic) : Easing.in(Easing.cubic),
      useNativeDriver: false,
    }).start(({ finished }) => {
      if (finished && onDone) onDone();
    });
  }, [searchAnim]);

  const openSearch = useCallback(() => {
    closingRef.current = false;
    setRenderSearch(true);
    setSearchOpen(true);
    runSearchAnim(1);
  }, [runSearchAnim]);

  const closeSearch = useCallback(() => {
    closingRef.current = true;
    inputRef.current?.blur();
    setSearchOpen(false);
    setQuery("");
    setMode("all");
    runSearchAnim(0, () => {
      if (closingRef.current) setRenderSearch(false);
    });
  }, [runSearchAnim]);

  useEffect(() => {
    if (!searchOpen) return;
    closeSearch();
  }, [closeSearch, pathname, searchOpen]);

  useEffect(() => {
    void refreshProStatus(true);
  }, [pathname, refreshProStatus]);

  const headerHeight = 56 + insets.top;
  const searchWidth = searchAnim.interpolate({
    inputRange: [0, 1],
    outputRange: [0, searchTargetWidth],
  });
  const searchContainerStyle = {
    width: searchWidth,
    opacity: searchAnim,
  } as const;

  return (
    <SafeAreaView style={styles.safeArea} edges={["top"]}>
      <View style={styles.container}>
        <TouchableOpacity style={styles.logoButton} onPress={() => router.replace("/(app)")}>
          <Image source={LOGO_IMAGE} style={styles.logo} resizeMode="cover" />
        </TouchableOpacity>

        <View style={styles.rightGroup}>
          {!renderSearch ? (
            <>
              <TouchableOpacity style={styles.iconButton} onPress={() => {}}>
                <Ionicons name="mail-outline" size={20} color={theme.colors.muted} />
              </TouchableOpacity>

              {isPro === false ? (
                <TouchableOpacity style={styles.proBadge} onPress={() => router.push("/paywall")}>
                  <FontAwesome5 name="crown" size={12} color={ACCENT} style={styles.proIcon} />
                  <Text style={styles.proText}>PRO</Text>
                </TouchableOpacity>
              ) : null}
            </>
          ) : null}

          {renderSearch ? (
            <Animated.View
              pointerEvents={searchOpen ? "auto" : "none"}
              style={[styles.searchFieldContainer, searchContainerStyle]}
            >
              <View style={styles.searchField}>
                <Ionicons name="search" size={16} color={theme.colors.muted} />
                <TextInput
                  ref={inputRef}
                  value={query}
                  onChangeText={setQuery}
                  placeholder="Search users or cards"
                  placeholderTextColor={theme.colors.muted}
                  style={styles.searchInput}
                  autoCorrect={false}
                  autoCapitalize="none"
                  returnKeyType="search"
                />
                <TouchableOpacity onPress={closeSearch} style={styles.searchClose}>
                  <Ionicons name="close" size={16} color={theme.colors.text} />
                </TouchableOpacity>
              </View>
            </Animated.View>
          ) : (
            <TouchableOpacity style={styles.iconButton} onPress={openSearch}>
              <Ionicons name="search" size={20} color={ACCENT} />
            </TouchableOpacity>
          )}
        </View>
      </View>

      <SearchResultsPanel
        visible={searchOpen}
        query={query}
        mode={mode}
        onModeChange={setMode}
        onClose={closeSearch}
        topOffset={headerHeight}
        panelWidth={searchTargetWidth}
        panelRightInset={HEADER_HORIZONTAL_PADDING}
      />
    </SafeAreaView>
  );
};

const styles = StyleSheet.create({
  safeArea: {
    backgroundColor: "rgba(2, 6, 23, 0.95)",
    borderBottomWidth: 1,
    borderBottomColor: "rgba(255, 255, 255, 0.08)",
    zIndex: 10,
  },
  container: {
    height: 56,
    paddingHorizontal: HEADER_HORIZONTAL_PADDING,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  logoButton: {
    width: 48,
    height: 48,
    borderRadius: 10,
    overflow: "hidden",
  },
  logo: {
    width: "100%",
    height: "100%",
    borderRadius: 10,
  },
  rightGroup: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
    justifyContent: "flex-end",
    flex: 1,
  },
  iconButton: {
    padding: 4,
    alignItems: "center",
    justifyContent: "center",
  },
  proBadge: {
    flexDirection: "row",
    alignItems: "center",
    backgroundColor: "rgba(0, 0, 0, 0.6)",
    paddingVertical: 6,
    paddingHorizontal: 10,
    borderRadius: 20,
    borderWidth: 1,
    borderColor: ACCENT,
  },
  proIcon: {
    marginRight: 6,
  },
  proText: {
    color: ACCENT,
    fontWeight: "800",
    fontSize: 12,
    letterSpacing: 0.4,
  },
  searchField: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 16,
    backgroundColor: "rgba(15, 23, 42, 0.9)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    width: "100%",
  },
  searchFieldContainer: {
    overflow: "hidden",
    justifyContent: "center",
  },
  searchInput: {
    flex: 1,
    color: theme.colors.text,
    fontSize: 13,
  },
  searchClose: {
    padding: 2,
  },
});
