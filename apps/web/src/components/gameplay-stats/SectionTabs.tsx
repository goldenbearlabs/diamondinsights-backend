"use client";

import type { SectionTab } from "./types";
import styles from "./styles.module.css";

const TABS: SectionTab[] = ["Analytics", "Game Log", "Cards", "Coaching"];

type Props = {
  activeTab: SectionTab;
  onChange: (tab: SectionTab) => void;
};

export function SectionTabs({ activeTab, onChange }: Props) {
  return (
    <section className={styles.sectionTabs}>
      {TABS.map((tab) => (
        <button
          key={tab}
          type="button"
          className={activeTab === tab ? styles.sectionTabActive : styles.sectionTab}
          onClick={() => onChange(tab)}
        >
          {tab}
        </button>
      ))}
    </section>
  );
}
