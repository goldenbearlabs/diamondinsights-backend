import type { Metadata } from "next";
import Link from "next/link";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";

import styles from "../legal.module.css";

const LAST_UPDATED = "March 11, 2026";

const SECTIONS = [
  {
    title: "Information We Collect",
    body: [
      "When you create or use a Diamond Insights account, we may collect information such as your email address, display name, profile photo, linked MLB The Show username, app activity, portfolio activity, predictions, messages, and support requests.",
      "We also collect technical information needed to operate the service, including device identifiers, approximate usage diagnostics, authentication records, and subscription status details provided through our payment and subscription providers.",
    ],
  },
  {
    title: "How We Use Information",
    body: [
      "We use your information to provide the app and website, authenticate your account, display your profile, calculate predictions and leaderboard results, personalize your experience, manage subscriptions, respond to support requests, and improve the service.",
      "We may also use aggregated or de-identified information for analytics, reliability monitoring, and product planning.",
    ],
  },
  {
    title: "How We Share Information",
    body: [
      "We do not sell your personal information. We may share information with service providers that help us run Diamond Insights, such as hosting, authentication, analytics, storage, and subscription infrastructure.",
      "If you choose to make profile or portfolio information public, that information may be visible to other users. We may also disclose information when required by law, to enforce our terms, or to protect the rights, safety, and security of Diamond Insights, our users, or the public.",
    ],
  },
  {
    title: "Subscriptions and Payments",
    body: [
      "Paid memberships are processed through the Apple App Store or Google Play, with subscription state managed through RevenueCat. We do not receive your full payment card details.",
      "We may store purchase status, entitlement information, renewal state, and related transaction metadata needed to provide premium features and customer support.",
    ],
  },
  {
    title: "Data Retention",
    body: [
      "We retain information for as long as reasonably necessary to operate the service, comply with legal obligations, resolve disputes, and enforce agreements.",
      "If you delete your account, we will remove or anonymize information within a reasonable period unless we need to retain certain records for security, fraud prevention, accounting, or legal compliance.",
    ],
  },
  {
    title: "Your Choices",
    body: [
      "You can update certain profile information in your account settings, manage subscription billing through your platform account, and request password reset emails from within the app.",
      "If you want to request deletion or have privacy questions, contact us at support@goldenbearlabs.com.",
    ],
  },
  {
    title: "Children's Privacy",
    body: [
      "Diamond Insights is not intended for children under 13, and we do not knowingly collect personal information from children under 13. If you believe a child has provided information to us, contact us so we can investigate and take appropriate action.",
    ],
  },
  {
    title: "Changes to This Policy",
    body: [
      "We may update this Privacy Policy from time to time. When we do, we will post the updated version on this page and revise the last updated date above.",
    ],
  },
] as const;

export const metadata: Metadata = {
  title: "Privacy Policy | Diamond Insights",
  description: "Privacy Policy for the Diamond Insights website and mobile app.",
};

export default function PrivacyPolicyPage() {
  return (
    <main className={styles.page}>
      <Navbar />
      <FloatingShieldsBackground />
      <div className={styles.texture} />

      <div className={styles.shell}>
        <header className={styles.hero}>
          <span className={styles.eyebrow}>Privacy Policy</span>
          <h1>Your data, explained in plain language.</h1>
          <p>
            This policy explains what information Diamond Insights collects, how we use it,
            when it may be shared, and what choices you have when using our website and mobile
            app.
          </p>
          <div className={styles.heroMeta}>
            <span className={styles.metaPill}>Last updated: {LAST_UPDATED}</span>
            <span className={styles.metaPill}>Applies to web and mobile</span>
          </div>
        </header>

        <div className={styles.grid}>
          <aside className={styles.sideCard}>
            <div>
              <h2>At a glance</h2>
              <p>
                Diamond Insights uses your information to run accounts, rankings, gameplay
                features, portfolios, subscriptions, and support.
              </p>
            </div>
            <div>
              <h2>Contact</h2>
              <p>support@goldenbearlabs.com</p>
            </div>
            <div>
              <h2>Key points</h2>
              <ul>
                <li>We do not sell your personal information.</li>
                <li>Public profile settings can make some information visible to other users.</li>
                <li>Subscription billing is handled by app store providers.</li>
              </ul>
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
            <Link href="/terms-and-conditions">Terms & Conditions</Link>
            <a href="mailto:support@goldenbearlabs.com">Contact</a>
          </div>
        </footer>
      </div>
    </main>
  );
}
