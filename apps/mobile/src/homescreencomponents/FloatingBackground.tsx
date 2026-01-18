import React, { useEffect, useRef, useState, useMemo } from 'react';
import { View, Animated, Dimensions, StyleSheet } from 'react-native';
import { theme } from '../theme/colors';

const { width, height } = Dimensions.get('window');

const PARTICLE_COUNT = 23; 
const ICON_SIZE = 35;
const CELL_SIZE = 70; 

const IMAGES = [
  require('../../assets/images/shield-bronze.png'),
  require('../../assets/images/shield-silver.png'),
  require('../../assets/images/shield-gold.png'),
  require('../../assets/images/shield-diamond.png'),
];

const COLS = Math.floor(width / CELL_SIZE);
const ROWS = Math.floor(height / CELL_SIZE);
const TOTAL_CELLS = COLS * ROWS;

type Position = {
  x: number;
  y: number;
  cellIndex: number; 
};

const DiamondParticle = ({ 
  initialPos, 
  onRequestNewPosition 
}: { 
  initialPos: Position, 
  onRequestNewPosition: (oldIndex: number) => Position 
}) => {
  const [pos, setPos] = useState<Position>(initialPos);
  
  const [currentImg, setCurrentImg] = useState(() => {
    return IMAGES[Math.floor(Math.random() * IMAGES.length)];
  });

  const opacity = useRef(new Animated.Value(0)).current; 

  useEffect(() => {
    let isMounted = true;

    const runCycle = () => {
      if (!isMounted) return;

      const inhaleTime = 2000 + Math.random() * 3000;
      const exhaleTime = 2000 + Math.random() * 3000;
      const maxOpacity = 0.3 + Math.random() * 0.4;
      
      const delay = Math.random() * 2000;

      Animated.sequence([
        Animated.delay(delay),
        Animated.timing(opacity, {
          toValue: maxOpacity,
          duration: inhaleTime,
          useNativeDriver: true,
        }),
        Animated.timing(opacity, {
          toValue: 0,
          duration: exhaleTime,
          useNativeDriver: true,
        }),
      ]).start(() => {
        if (isMounted) {
          const newPos = onRequestNewPosition(pos.cellIndex);
          setPos(newPos);

          setCurrentImg((prev: any) => {
            const opts = IMAGES.filter(i => i !== prev);
            return opts[Math.floor(Math.random() * opts.length)];
          });

          runCycle();
        }
      });
    };

    runCycle();

    return () => { isMounted = false; };
  }, [pos.cellIndex]); 

  return (
    <Animated.Image
      source={currentImg}
      style={{
        position: 'absolute',
        left: pos.x,
        top: pos.y,
        width: ICON_SIZE,
        height: ICON_SIZE,
        opacity: opacity,
      }}
      resizeMode="contain"
    />
  );
};

export const FloatingBackground = () => {
  const occupiedCells = useRef<Set<number>>(new Set()).current;

  const getCoordinatesForCell = (index: number) => {
    const col = index % COLS;
    const row = Math.floor(index / COLS);
    
    return {
      x: (col * CELL_SIZE) + (Math.random() * (CELL_SIZE - ICON_SIZE)),
      y: (row * CELL_SIZE) + (Math.random() * (CELL_SIZE - ICON_SIZE)),
      cellIndex: index,
    };
  };

  const findFreeCell = (excludeIndex: number | null): Position => {
    if (excludeIndex !== null) occupiedCells.delete(excludeIndex);

    const freeSlots = [];
    for (let i = 0; i < TOTAL_CELLS; i++) {
      if (!occupiedCells.has(i)) freeSlots.push(i);
    }

    if (freeSlots.length === 0) {
        return getCoordinatesForCell(excludeIndex || 0);
    }

    const luckyIndex = freeSlots[Math.floor(Math.random() * freeSlots.length)];
    
    occupiedCells.add(luckyIndex);
    return getCoordinatesForCell(luckyIndex);
  };

  const initialPositions = useMemo(() => {
    const positions: Position[] = [];
    for (let i = 0; i < PARTICLE_COUNT; i++) {
      const pos = findFreeCell(null);
      positions.push(pos);
    }
    return positions;
  }, []);

  return (
    <View style={StyleSheet.absoluteFillObject} pointerEvents="none">
      {initialPositions.map((startPos, i) => (
        <DiamondParticle 
          key={i} 
          initialPos={startPos}
          onRequestNewPosition={(oldIndex) => findFreeCell(oldIndex)}
        />
      ))}
      
      <View style={[StyleSheet.absoluteFillObject, { backgroundColor: theme.colors.background, opacity: 0.15 }]} />
    </View>
  );
};