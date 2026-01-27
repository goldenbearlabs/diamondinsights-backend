import React from 'react';
import { View, Image, Text, StyleSheet } from 'react-native';
import { theme } from '../theme/colors';

type HeaderBarProps = {
  title?: string;
  rightElement?: React.ReactNode;
};

export const HeaderBar = ({ title, rightElement }: HeaderBarProps) => {
  return (
    <View style={styles.container}>
      <View style={styles.leftGroup}>
        <Image
          source={require('../../assets/images/placeholder.png')}
          style={styles.logo}
          resizeMode="cover"
        />
        {title ? <Text style={styles.title}>{title}</Text> : null}
      </View>
      <View style={styles.rightGroup}>{rightElement}</View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    height: 56,
    paddingHorizontal: 16,
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    backgroundColor: 'rgba(2, 6, 23, 0.7)',
    borderBottomWidth: 1,
    borderBottomColor: 'rgba(255, 255, 255, 0.08)',
  },
  leftGroup: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
  },
  logo: {
    width: 36,
    height: 36,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
  },
  title: {
    color: theme.colors.text,
    fontSize: 16,
    fontWeight: '700',
    letterSpacing: 0.3,
  },
  rightGroup: {
    minWidth: 24,
    alignItems: 'flex-end',
  },
});
