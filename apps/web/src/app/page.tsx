"use client";

import Link from "next/link";
import Image from "next/image";
import { useEffect, useMemo, useState } from "react";
import { onAuthStateChanged } from "firebase/auth";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import { getFirebaseAuth } from "@/lib/firebase";

import styles from "./page.module.css";

type CardPreview = {
  id: string;
  name: string;
  ovr: number;
  image: string;
};

type Countdown = {
  d: number;
  h: number;
  m: number;
  s: number;
};

const COUNTDOWN_START_SECONDS = 4 * 24 * 60 * 60 + 12 * 60 * 60 + 30 * 60;

const FALLBACK_CARDS: CardPreview[] = [
  { id: "fallback-1", name: "Elite Contact Hitter", ovr: 92, image: "" },
  { id: "fallback-2", name: "Power Corner Bat", ovr: 91, image: "" },
  { id: "fallback-3", name: "Ace Pitching Prospect", ovr: 90, image: "" },
];

const HOW_IT_WORKS_STEPS = [
  {
    title: "Data Collection",
    description: "We aggregate real-time player stats and historical trends.",
    colorClass: styles.stepBlue,
  },
  {
    title: "AI Analysis",
    description: "Our model finds upgrade signals before market sentiment catches up.",
    colorClass: styles.stepIndigo,
  },
  {
    title: "Profit",
    description: "Use projection edges to make smarter buy, hold, and flip decisions.",
    colorClass: styles.stepGreen,
  },
] as const;

const TRUST_STATS = [
  { value: "96%", label: "Model Accuracy" },
  { value: "17k+", label: "Players Analyzed" },
  { value: "24/7", label: "Market Updates" },
] as const;
const CAROUSEL_ROTATE_MS = 3200;
const CURRENT_YEAR = new Date().getFullYear();

function toCountdown(totalSeconds: number): Countdown {
  const d = Math.floor(totalSeconds / 86_400);
  const remainderAfterDays = totalSeconds % 86_400;
  const h = Math.floor(remainderAfterDays / 3_600);
  const remainderAfterHours = remainderAfterDays % 3_600;
  const m = Math.floor(remainderAfterHours / 60);
  const s = remainderAfterHours % 60;
  return { d, h, m, s };
}

function getFakePrediction(baseOvr: number, id: string): string {
  let hash = 0;
  for (let i = 0; i < id.length; i += 1) {
    hash = id.charCodeAt(i) + ((hash << 5) - hash);
  }
  const randomBoost = 1 + (Math.abs(hash) % 300) / 100;
  return (baseOvr + randomBoost - 1).toFixed(2);
}

function countdownLabel(countdown: Countdown): string {
  return `${countdown.d}d ${countdown.h}h ${countdown.m}m ${countdown.s}s`;
}

function getCarouselOffset(index: number, activeIndex: number, total: number): number {
  if (total <= 1) {
    return 0;
  }
  let offset = index - activeIndex;
  if (offset > total / 2) {
    offset -= total;
  } else if (offset < -total / 2) {
    offset += total;
  }
  return offset;
}

export default function Home() {
  const [countdownSeconds, setCountdownSeconds] = useState(COUNTDOWN_START_SECONDS);
  const [previewCards, setPreviewCards] = useState<CardPreview[]>([]);
  const [loadingCards, setLoadingCards] = useState(true);
  const [activeCardIndex, setActiveCardIndex] = useState(0);
  const [authCtaHref, setAuthCtaHref] = useState("/signin");
  const [authCtaLabel, setAuthCtaLabel] = useState("Sign in / Create Account");

  useEffect(() => {
    let unsubscribe: (() => void) | null = null;
    try {
      const auth = getFirebaseAuth();
      unsubscribe = onAuthStateChanged(auth, (user) => {
        if (user) {
          setAuthCtaHref("/account");
          setAuthCtaLabel("Open Account");
        } else {
          setAuthCtaHref("/signin");
          setAuthCtaLabel("Sign in / Create Account");
        }
      });
    } catch {
      setAuthCtaHref("/signin");
      setAuthCtaLabel("Sign in / Create Account");
    }

    return () => {
      if (unsubscribe) {
        unsubscribe();
      }
    };
  }, []);

  useEffect(() => {
    const interval = window.setInterval(() => {
      setCountdownSeconds((prev) => Math.max(0, prev - 1));
    }, 1000);

    return () => window.clearInterval(interval);
  }, []);

  useEffect(() => {
    let active = true;

    async function loadCards() {
      try {
        const response = await fetch("/api/home/cards-preview", { cache: "no-store" });
        if (!response.ok) {
          throw new Error("Card preview unavailable");
        }

        const payload = (await response.json()) as CardPreview[];
        if (!active) {
          return;
        }

        if (!Array.isArray(payload) || payload.length === 0) {
          setPreviewCards(FALLBACK_CARDS);
          return;
        }

        setPreviewCards(payload);
      } catch {
        if (active) {
          setPreviewCards(FALLBACK_CARDS);
        }
      } finally {
        if (active) {
          setLoadingCards(false);
        }
      }
    }

    void loadCards();
    return () => {
      active = false;
    };
  }, []);

  const cards = previewCards.length > 0 ? previewCards : FALLBACK_CARDS;
  const cardCount = cards.length;

  useEffect(() => {
    if (cardCount <= 1) {
      return;
    }

    const interval = window.setInterval(() => {
      setActiveCardIndex((prev) => (prev + 1) % cardCount);
    }, CAROUSEL_ROTATE_MS);

    return () => window.clearInterval(interval);
  }, [cardCount]);

  const countdown = useMemo(() => toCountdown(countdownSeconds), [countdownSeconds]);
  const safeActiveCardIndex = cards.length > 0 ? activeCardIndex % cards.length : 0;

  return (
    <main className={styles.page}>
      <Navbar />
      <FloatingShieldsBackground />
      <div className={styles.texture} />

      <div className={styles.content}>
        <section className={styles.mainCard}>
          <div className={styles.heroHeader}>
            <h1>
              Diamond<span>Insights</span>
            </h1>
            <p>
              The <strong>#1</strong> app for dominating MLB The Show.
            </p>
          </div>

          <div className={styles.countdownPill}>
            <span>NEXT UPDATE:</span>
            <strong>{countdownLabel(countdown)}</strong>
          </div>

          <div className={styles.buttonGroup}>
            <Link href={authCtaHref} className={styles.primaryButton}>
              {authCtaLabel}
            </Link>
            <a href="#how-it-works" className={styles.secondaryButton}>
              View Our AI-Powered Predictions
            </a>
          </div>

          <section className={styles.predictionSection} aria-live="polite">
            <div className={styles.predictionTrack}>
              {cards.map((card, index) => {
                const offset = getCarouselOffset(index, safeActiveCardIndex, cards.length);
                const absOffset = Math.abs(offset);
                const translateX = absOffset === 0 ? 0 : absOffset === 1 ? offset * 210 : offset * 260;
                const translateY = absOffset === 0 ? 0 : absOffset === 1 ? 28 : 44;
                const scale = absOffset === 0 ? 1 : absOffset === 1 ? 0.82 : 0.68;
                const opacity = absOffset === 0 ? 1 : absOffset === 1 ? 0.42 : 0;
                const blur = absOffset === 0 ? 0 : absOffset === 1 ? 1.2 : 2.8;
                const zIndex = 20 - absOffset;
                const isVisibleLayer = absOffset <= 1;

                return (
                  <article
                    key={card.id}
                    className={`${styles.predictionCard} ${absOffset === 0 ? styles.predictionCardActive : ""}`}
                    style={{
                      transform: `translateX(calc(-50% + ${translateX}px)) translateY(${translateY}px) scale(${scale})`,
                      opacity,
                      filter: `blur(${blur}px)`,
                      zIndex,
                      pointerEvents: isVisibleLayer ? "auto" : "none",
                    }}
                    aria-hidden={!isVisibleLayer}
                  >
                    <div className={styles.cardImageWrap}>
                      {card.image ? (
                        <Image
                          src={card.image}
                          alt={card.name}
                          className={styles.cardImage}
                          width={340}
                          height={460}
                          loading="lazy"
                          unoptimized
                        />
                      ) : (
                        <div className={styles.cardImageFallback}>
                          <span>{card.ovr}</span>
                          <small>OVR</small>
                        </div>
                      )}
                    </div>
                    <div className={styles.predictionPill}>
                      <p>
                        <span>{card.ovr}</span> <b>→</b> <strong>{getFakePrediction(card.ovr, card.id)}</strong>
                      </p>
                      <small>AI PREDICTION EXAMPLE</small>
                    </div>
                    <p className={styles.cardName}>{card.name}</p>
                  </article>
                );
              })}
            </div>
            <p className={styles.disclaimer}>
              {loadingCards
                ? "Loading live card previews..."
                : "*Get Pro to see all live market predictions and real-time signals."}
            </p>
          </section>
        </section>

        <section className={styles.mainCard}>
          <div className={styles.statsRow}>
            {TRUST_STATS.map((stat) => (
              <div key={stat.label} className={styles.statItem}>
                <strong>{stat.value}</strong>
                <span>{stat.label}</span>
              </div>
            ))}
          </div>

          <div id="how-it-works" className={styles.howItWorks}>
            <h2>HOW IT WORKS</h2>
            <div className={styles.steps}>
              {HOW_IT_WORKS_STEPS.map((step, index) => (
                <article key={step.title} className={styles.stepCard}>
                  <div className={`${styles.stepIcon} ${step.colorClass}`}>{index + 1}</div>
                  <div>
                    <h3>{step.title}</h3>
                    <p>{step.description}</p>
                  </div>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section className={styles.contactCard}>
          <div className={styles.contactSection}>
            <p>Have any questions?</p>
            <a href="mailto:support@goldenbearlabs.com">support@goldenbearlabs.com</a>
          </div>

          <div className={styles.contactDivider} />

          <div className={styles.socialSection}>
            <p>Follow us on social media</p>
            <div className={styles.socialLinks}>
              <a href="https://www.instagram.com/diamondinsights.app/" target="_blank" rel="noreferrer">
                Instagram
              </a>
              <a href="https://x.com/goldenbearlabs" target="_blank" rel="noreferrer">
                X
              </a>
            </div>
          </div>
        </section>

        <footer className={styles.footer}>
          <p>&copy; {CURRENT_YEAR} Diamond Insights by Golden Bear Labs</p>
          <div className={styles.footerLinks}>
            <a href="#how-it-works">How It Works</a>
            <Link href="/privacy-policy">Privacy Policy</Link>
            <Link href="/terms-and-conditions">Terms & Conditions</Link>
            <a href="mailto:support@goldenbearlabs.com">Contact</a>
            <a href="https://x.com/goldenbearlabs" target="_blank" rel="noreferrer">
              X
            </a>
          </div>
        </footer>
      </div>
    </main>
  );
}
