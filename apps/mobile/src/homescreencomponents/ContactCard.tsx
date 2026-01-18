import React from 'react';
import { View, Text, TouchableOpacity, StyleSheet, Linking } from 'react-native';
import { FontAwesome5 } from '@expo/vector-icons';
import { theme } from '../theme/colors'; 

export const ContactCard = () => {
  return (
    <View style={styles.card}>
      
      {/* Email Section */}
      <View style={styles.emailSection}>
        <Text style={styles.contactLabel}>Have any questions?</Text>
        <TouchableOpacity onPress={() => Linking.openURL('mailto:support@goldenbearlabs.com')}>
          <Text style={styles.emailText}>support@goldenbearlabs.com</Text>
        </TouchableOpacity>
      </View>

    
      <View style={styles.divider} />

      {/* Socials Section */}
      <View style={styles.socialSection}>
        <Text style={styles.socialLabel}>
          Follow us on Social Media
        </Text>
        
        <View style={styles.socialRow}>
          {/* Instagram Button */}
          <TouchableOpacity 
            style={styles.socialBtn} 
            onPress={() => Linking.openURL('https://www.instagram.com/diamondinsights.app/')}
          >
            <FontAwesome5 name="instagram" size={24} color="white" />
          </TouchableOpacity>

          {/* X (Twitter) Button */}
          <TouchableOpacity 
            style={styles.socialBtn}
            onPress={() => Linking.openURL('https://x.com/goldenbearlabs')}
          >
            <FontAwesome5 name="twitter" size={24} color="white" />
          </TouchableOpacity>
        </View>
      </View>

    </View>
  );
};

const styles = StyleSheet.create({
  card: {
    width: '100%',
    marginTop: 24, 
    backgroundColor: 'rgba(2, 6, 23, 0.7)',
    borderRadius: 24,
    paddingVertical: 32,
    paddingHorizontal: 16,
    alignItems: 'center',
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 10 },
    shadowOpacity: 0.3,
    shadowRadius: 20,
  },
  
  // Internal Layout
  emailSection: {
    alignItems: 'center',
    marginBottom: 28,
  },
  divider: {
    width: '40%',
    height: 1,
    backgroundColor: 'rgba(255,255,255,0.1)',
    marginBottom: 28,
  },
  socialSection: {
    alignItems: 'center',
  },
  socialRow: {
    flexDirection: 'row',
    gap: 20,
  },

 
  contactLabel: {
    color: theme.colors.muted,
    fontSize: 16,
    fontWeight: '600',
    marginBottom: 6,
  },
  emailText: {
    color: '#fbbf24', 
    fontSize: 18,
    fontWeight: '700',
    textDecorationLine: 'underline',
  },
  socialLabel: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: '700',
    textTransform: 'uppercase',
    letterSpacing: 1,
    marginBottom: 16,
  },
  socialBtn: {
    width: 50,
    height: 50,
    borderRadius: 25,
    backgroundColor: 'rgba(255, 255, 255, 0.05)',
    justifyContent: 'center',
    alignItems: 'center',
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
  },
});