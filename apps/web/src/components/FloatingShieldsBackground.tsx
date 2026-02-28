"use client";

import Image from "next/image";
import { useEffect, useRef, useState } from "react";

import styles from "./FloatingShieldsBackground.module.css";

const PARTICLE_COUNT = 23;
const ICON_SIZE = 35;
const CELL_SIZE = 70;
const MIN_WIDTH = 320;
const MIN_HEIGHT = 560;

const SHIELDS = [
  "/images/shields/shield-bronze.png",
  "/images/shields/shield-silver.png",
  "/images/shields/shield-gold.png",
  "/images/shields/shield-diamond.png",
] as const;

type GridBounds = {
  width: number;
  height: number;
  cols: number;
  rows: number;
  totalCells: number;
};

type Particle = {
  id: number;
  x: number;
  y: number;
  cellIndex: number;
  shieldIndex: number;
  opacity: number;
  transitionMs: number;
  size: number;
  rotation: number;
};

function randomInt(maxExclusive: number): number {
  return Math.floor(Math.random() * maxExclusive);
}

function randomFromRange(min: number, max: number): number {
  return min + Math.random() * (max - min);
}

function getBounds(): GridBounds {
  const width = Math.max(window.innerWidth, MIN_WIDTH);
  const height = Math.max(window.innerHeight, MIN_HEIGHT);
  const cols = Math.max(1, Math.floor(width / CELL_SIZE));
  const rows = Math.max(1, Math.floor(height / CELL_SIZE));
  const totalCells = cols * rows;
  return { width, height, cols, rows, totalCells };
}

function getCoordsForCell(index: number, bounds: GridBounds, size: number): { x: number; y: number } {
  const col = index % bounds.cols;
  const row = Math.floor(index / bounds.cols);
  const jitterX = Math.max(0, CELL_SIZE - size);
  const jitterY = Math.max(0, CELL_SIZE - size);

  return {
    x: col * CELL_SIZE + randomFromRange(0, jitterX),
    y: row * CELL_SIZE + randomFromRange(0, jitterY),
  };
}

export function FloatingShieldsBackground() {
  const [particles, setParticles] = useState<Particle[]>([]);
  const boundsRef = useRef<GridBounds | null>(null);
  const occupiedCellsRef = useRef<Set<number>>(new Set());
  const timersRef = useRef<Map<number, number[]>>(new Map());
  const resizeTimerRef = useRef<number | null>(null);

  useEffect(() => {
    let cancelled = false;

    const clearParticleTimers = (particleId: number) => {
      const timers = timersRef.current.get(particleId) ?? [];
      for (const timerId of timers) {
        window.clearTimeout(timerId);
      }
      timersRef.current.set(particleId, []);
    };

    const clearAllTimers = () => {
      for (const timers of timersRef.current.values()) {
        for (const timerId of timers) {
          window.clearTimeout(timerId);
        }
      }
      timersRef.current.clear();

      if (resizeTimerRef.current !== null) {
        window.clearTimeout(resizeTimerRef.current);
        resizeTimerRef.current = null;
      }
    };

    const pushTimer = (particleId: number, timerId: number) => {
      const existing = timersRef.current.get(particleId) ?? [];
      existing.push(timerId);
      timersRef.current.set(particleId, existing);
    };

    const findFreeCell = (excludeCell: number | null): number => {
      const bounds = boundsRef.current;
      if (!bounds) {
        return 0;
      }

      if (excludeCell !== null) {
        occupiedCellsRef.current.delete(excludeCell);
      }

      const free: number[] = [];
      for (let i = 0; i < bounds.totalCells; i += 1) {
        if (!occupiedCellsRef.current.has(i)) {
          free.push(i);
        }
      }

      const cellIndex = free.length > 0 ? free[randomInt(free.length)] : Math.max(excludeCell ?? 0, 0);
      occupiedCellsRef.current.add(cellIndex);
      return cellIndex;
    };

    const runParticleCycle = (particleId: number, cellIndex: number, shieldIndex: number) => {
      if (cancelled) {
        return;
      }

      clearParticleTimers(particleId);

      const delayMs = randomFromRange(0, 2000);
      const inhaleMs = randomFromRange(2000, 5000);
      const exhaleMs = randomFromRange(2000, 5000);
      const maxOpacity = randomFromRange(0.3, 0.7);

      const startInhale = window.setTimeout(() => {
        setParticles((prev) =>
          prev.map((particle) =>
            particle.id === particleId
              ? { ...particle, opacity: maxOpacity, transitionMs: inhaleMs }
              : particle,
          ),
        );
      }, delayMs);
      pushTimer(particleId, startInhale);

      const startExhale = window.setTimeout(() => {
        setParticles((prev) =>
          prev.map((particle) =>
            particle.id === particleId
              ? { ...particle, opacity: 0, transitionMs: exhaleMs }
              : particle,
          ),
        );
      }, delayMs + inhaleMs);
      pushTimer(particleId, startExhale);

      const reassign = window.setTimeout(() => {
        if (cancelled) {
          return;
        }

        const bounds = boundsRef.current;
        if (!bounds) {
          return;
        }

        const nextCell = findFreeCell(cellIndex);
        const nextShield = (shieldIndex + 1 + randomInt(SHIELDS.length - 1)) % SHIELDS.length;
        const nextSize = ICON_SIZE + randomFromRange(-4, 6);
        const nextRotation = randomFromRange(-20, 20);
        const coords = getCoordsForCell(nextCell, bounds, nextSize);

        setParticles((prev) =>
          prev.map((particle) =>
            particle.id === particleId
              ? {
                  ...particle,
                  x: coords.x,
                  y: coords.y,
                  cellIndex: nextCell,
                  shieldIndex: nextShield,
                  size: nextSize,
                  rotation: nextRotation,
                  opacity: 0,
                  transitionMs: 0,
                }
              : particle,
          ),
        );

        runParticleCycle(particleId, nextCell, nextShield);
      }, delayMs + inhaleMs + exhaleMs);
      pushTimer(particleId, reassign);
    };

    const setupParticles = () => {
      boundsRef.current = getBounds();
      occupiedCellsRef.current.clear();
      clearAllTimers();

      const bounds = boundsRef.current;
      if (!bounds) {
        return;
      }

      const nextParticles: Particle[] = [];
      for (let i = 0; i < PARTICLE_COUNT; i += 1) {
        const cellIndex = findFreeCell(null);
        const size = ICON_SIZE + randomFromRange(-4, 6);
        const coords = getCoordsForCell(cellIndex, bounds, size);

        nextParticles.push({
          id: i,
          x: coords.x,
          y: coords.y,
          cellIndex,
          shieldIndex: randomInt(SHIELDS.length),
          opacity: 0,
          transitionMs: 0,
          size,
          rotation: randomFromRange(-20, 20),
        });
      }

      setParticles(nextParticles);
      for (const particle of nextParticles) {
        runParticleCycle(particle.id, particle.cellIndex, particle.shieldIndex);
      }
    };

    setupParticles();

    const onResize = () => {
      if (resizeTimerRef.current !== null) {
        window.clearTimeout(resizeTimerRef.current);
      }
      resizeTimerRef.current = window.setTimeout(() => {
        setupParticles();
      }, 180);
    };

    window.addEventListener("resize", onResize);

    return () => {
      cancelled = true;
      window.removeEventListener("resize", onResize);
      clearAllTimers();
    };
  }, []);

  return (
    <div className={styles.background} aria-hidden>
      {particles.map((particle) => (
        <div
          key={particle.id}
          className={styles.particle}
          style={{
            left: particle.x,
            top: particle.y,
            opacity: particle.opacity,
            width: particle.size,
            height: particle.size,
            transform: `rotate(${particle.rotation}deg)`,
            transitionDuration: `${Math.max(0, particle.transitionMs)}ms`,
          }}
        >
          <Image
            src={SHIELDS[particle.shieldIndex]}
            alt=""
            width={64}
            height={64}
            className={styles.particleImage}
            priority={false}
          />
        </div>
      ))}
      <div className={styles.overlay} />
    </div>
  );
}
