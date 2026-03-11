import type { Metadata } from "next";
import Link from "next/link";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";

import styles from "../legal.module.css";

const LAST_UPDATED = "March 11, 2026";

const SECTIONS = [
  {
    title: "Acceptance of Terms",
    body: [
      "By accessing or using Diamond Insights, you agree to these Terms & Conditions. If you do not agree, do not use the service.",
      "These terms apply to the Diamond Insights website, mobile app, and related services made available by Golden Bear Labs.",
    ],
  },
  {
    title: "Accounts",
    body: [
      "You are responsible for maintaining the confidentiality of your login credentials and for activity that occurs under your account.",
      "You agree to provide accurate account information and keep it reasonably up to date.",
    ],
  },
  {
    title: "Subscriptions and Paid Features",
    body: [
      "Certain features may require a paid subscription. Pricing, billing cycles, renewal terms, cancellation handling, and refunds are governed by the app store platform through which you purchased your subscription, subject to applicable law.",
      "We may change, add, or remove features included in a subscription offering as the product evolves.",
    ],
  },
  {
    title: "Permitted Use",
    body: [
      "You may use Diamond Insights only for lawful purposes and in compliance with these terms. You may not misuse the service, interfere with its operation, attempt unauthorized access, scrape or copy protected data at scale, or use the platform to harass, spam, or impersonate others.",
      "We may suspend or terminate access if we believe your conduct creates risk for the service, other users, or our business.",
    ],
  },
  {
    title: "Predictions and Informational Content",
    body: [
      "Diamond Insights provides rankings, analytics, predictions, and other informational content for entertainment and decision-support purposes only. We do not guarantee accuracy, completeness, profitability, or any specific outcome from using the service.",
      "You remain solely responsible for your own decisions, including any in-game, marketplace, financial, or subscription decisions you make based on information shown in the product.",
    ],
  },
  {
    title: "User Content and Public Profiles",
    body: [
      "If you submit content, messages, usernames, profile information, or other material through Diamond Insights, you represent that you have the right to do so and that the content does not violate the law or the rights of others.",
      "You grant us a non-exclusive license to host, store, display, and process that content as needed to operate and improve the service.",
    ],
  },
  {
    title: "Intellectual Property",
    body: [
      "Diamond Insights, including its software, branding, design, and original content, is owned by Golden Bear Labs or its licensors and is protected by applicable intellectual property laws.",
      "These terms do not give you ownership of the service or any right to reproduce, distribute, or create derivative works from our protected materials except as expressly permitted by law.",
    ],
  },
  {
    title: "Disclaimers and Limitation of Liability",
    body: [
      "Diamond Insights is provided on an as available and as is basis to the fullest extent permitted by law. We disclaim warranties of merchantability, fitness for a particular purpose, non-infringement, and uninterrupted availability.",
      "To the fullest extent permitted by law, Golden Bear Labs will not be liable for indirect, incidental, special, consequential, exemplary, or punitive damages, or for loss of profits, data, goodwill, or business interruption arising from or related to your use of the service.",
    ],
  },
  {
    title: "Changes and Contact",
    body: [
      "We may update these terms from time to time by posting a revised version on this page. Your continued use of Diamond Insights after changes take effect means you accept the updated terms.",
      "Questions about these terms can be sent to support@goldenbearlabs.com.",
    ],
  },
] as const;

export const metadata: Metadata = {
  title: "Terms & Conditions | Diamond Insights",
  description: "Terms & Conditions for the Diamond Insights website and mobile app.",
};

export default function TermsAndConditionsPage() {
  return (
    <main className={styles.page}>
      <Navbar />
      <FloatingShieldsBackground />
      <div className={styles.texture} />

      <div className={styles.shell}>
        <header className={styles.hero}>
          <span className={styles.eyebrow}>Terms & Conditions</span>
          <h1>The rules for using Diamond Insights.</h1>
          <p>
            These terms explain how Diamond Insights may be used, how subscriptions work, what
            responsibilities users keep, and the legal limits that apply to the service.
          </p>
          <div className={styles.heroMeta}>
            <span className={styles.metaPill}>Last updated: {LAST_UPDATED}</span>
            <span className={styles.metaPill}>Golden Bear Labs</span>
          </div>
        </header>

        <div className={styles.grid}>
          <aside className={styles.sideCard}>
            <div>
              <h2>Applies to</h2>
              <p>Diamond Insights on the web, in the mobile app, and in related paid features.</p>
            </div>
            <div>
              <h2>Important</h2>
              <ul>
                <li>Predictions and analytics are not guaranteed outcomes.</li>
                <li>App store platforms control subscription billing and refunds.</li>
                <li>We may suspend accounts that misuse the service.</li>
              </ul>
            </div>
            <div>
              <h2>Contact</h2>
              <p>support@goldenbearlabs.com</p>
            </div>
          </aside>

          <article className={styles.documentCard}>
            {SECTIONS.map((section) => (
              <section key={section.title} className={styles.documentSection}>
                <h2>{section.title}</h2>
                {section.body.map((paragraph) => (
                  <p key={paragraph}>{paragraph}</p>
                ))}
              </section>
            ))}
          </article>
        </div>

        <footer className={styles.footer}>
          <p>&copy; 2026 Diamond Insights by Golden Bear Labs</p>
          <div className={styles.footerLinks}>
            <Link href="/">Home</Link>
            <Link href="/privacy-policy">Privacy Policy</Link>
            <a href="mailto:support@goldenbearlabs.com">Contact</a>
          </div>
        </footer>
      </div>
    </main>
  );
}
