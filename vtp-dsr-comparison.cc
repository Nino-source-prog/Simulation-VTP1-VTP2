/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * vtp-dsr-comparison.cc
 *
 * Comparaison de trois politiques de retransmission dans un MANET DSR :
 *   - TRAD : retransmission sur expiration du RTO avec backoff exponentiel ;
 *   - VTP1 : retransmission conditionnee par l'etat du canal C(t) ;
 *   - VTP2 : retransmission conditionnee par C(t) ET la disponibilite du
 *            chemin DSR P(t).
 *
 * Le transport est un modele SCTP-like construit sur UDP : TSN simplifie,
 * ACK, file de retransmission, T3-rtx abstrait, backoff et duree de vie.
 * Il ne s'agit pas d'une implementation complete de SCTP.
 *
 * Le fichier suppose le meme patch DSR que vtp-doung.cc pour les traces :
 *   RreqInit(Ipv4Address src, Ipv4Address dst, uint32_t requestId, Time t)
 *   RouteDiscoveryCompleted(src,dst,requestId,t)
 *   RouteDiscoveryFailed(src,dst,requestId,t)
 *   RreqTimerArmed(Ipv4Address src, Ipv4Address dst, uint32_t id,
 *                   Time retryDelay, Time armedTime)
 *   RreqTimeout(Ipv4Address src, Ipv4Address dst, uint32_t id, Time t)
 *
 * TR est mesure exactement comme l'intervalle armement -> expiration du
 * meme timer RREQ, associe par (source, destination, requestId). La duree
 * complete premier RREQ -> RREP est exportee separement.
 */

#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/mobility-module.h"
#include "ns3/wifi-module.h"
#include "ns3/dsr-module.h"
#include "ns3/dsr-routing.h"
#include "ns3/dsr-rcache.h"
#include "ns3/applications-module.h"
#include "ns3/energy-module.h"
#include "ns3/wifi-radio-energy-model-helper.h"
#include "ns3/ipv4-l3-protocol.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE ("VtpDsrComparison");

static double
ToMilliseconds (Time value)
{
  return static_cast<double> (value.GetNanoSeconds ()) / 1e6;
}

// ===========================================================================
// Tags conserves dans toute la pile ns-3
// ===========================================================================

class TimestampTag : public Tag
{
public:
  static TypeId GetTypeId ()
  {
    static TypeId tid = TypeId ("VtpTimestampTag")
      .SetParent<Tag> ()
      .AddConstructor<TimestampTag> ();
    return tid;
  }

  TypeId GetInstanceTypeId () const override { return GetTypeId (); }
  uint32_t GetSerializedSize () const override { return 8; }
  void Serialize (TagBuffer b) const override { b.WriteU64 (m_time.GetNanoSeconds ()); }
  void Deserialize (TagBuffer b) override { m_time = NanoSeconds (b.ReadU64 ()); }
  void Print (std::ostream& os) const override { os << m_time; }

  void SetTime (Time t) { m_time = t; }
  Time GetTime () const { return m_time; }

private:
  Time m_time;
};

class SeqTag : public Tag
{
public:
  static TypeId GetTypeId ()
  {
    static TypeId tid = TypeId ("VtpSeqTag")
      .SetParent<Tag> ()
      .AddConstructor<SeqTag> ();
    return tid;
  }

  TypeId GetInstanceTypeId () const override { return GetTypeId (); }
  uint32_t GetSerializedSize () const override { return 4; }
  void Serialize (TagBuffer b) const override { b.WriteU32 (m_seq); }
  void Deserialize (TagBuffer b) override { m_seq = b.ReadU32 (); }
  void Print (std::ostream& os) const override { os << m_seq; }

  void SetSeq (uint32_t seq) { m_seq = seq; }
  uint32_t GetSeq () const { return m_seq; }

private:
  uint32_t m_seq = 0;
};

class AttemptTag : public Tag
{
public:
  static TypeId GetTypeId ()
  {
    static TypeId tid = TypeId ("VtpAttemptTag")
      .SetParent<Tag> ()
      .AddConstructor<AttemptTag> ();
    return tid;
  }

  TypeId GetInstanceTypeId () const override { return GetTypeId (); }
  uint32_t GetSerializedSize () const override { return 5; }

  void Serialize (TagBuffer b) const override
  {
    b.WriteU32 (m_attemptId);
    b.WriteU8 (m_retransmission ? 1 : 0);
  }

  void Deserialize (TagBuffer b) override
  {
    m_attemptId = b.ReadU32 ();
    m_retransmission = (b.ReadU8 () != 0);
  }

  void Print (std::ostream& os) const override
  {
    os << "attempt=" << m_attemptId << ",retx=" << m_retransmission;
  }

  void Set (uint32_t id, bool retransmission)
  {
    m_attemptId = id;
    m_retransmission = retransmission;
  }

  uint32_t GetAttemptId () const { return m_attemptId; }
  bool IsRetransmission () const { return m_retransmission; }

private:
  uint32_t m_attemptId = 0;
  bool m_retransmission = false;
};

// ===========================================================================
// Environnement cross-layer : canal C(t) et route P(t)
// ===========================================================================

class VtpEnvironment
{
public:
  typedef Callback<void, bool, Time> StateCallback;

  static void Reset ()
  {
    s_channelAvailable = true;
    s_routeAvailable = false;
    s_forceRouteAvailable = false;
    s_lastChannelUp = Seconds (0);
    s_lastRouteUp = Seconds (0);
    s_outageStart = Seconds (0);
    s_totalChannelOutage = Seconds (0);
    s_channelOutages = 0;
    s_channelListeners.clear ();
    s_routeListeners.clear ();
  }

  static bool ChannelAvailable () { return s_channelAvailable; }
  static bool RouteAvailable ()
  {
    return s_forceRouteAvailable ? true : s_routeAvailable;
  }
  static void SetForceRouteAvailable (bool force)
  {
    s_forceRouteAvailable = force;
    if (force)
      {
        SetRouteAvailable (true, Simulator::Now ());
      }
  }
  static bool ForceRouteAvailable () { return s_forceRouteAvailable; }
  static Time LastChannelUp () { return s_lastChannelUp; }
  static Time LastRouteUp () { return s_lastRouteUp; }
  static Time TotalChannelOutage () { return s_totalChannelOutage; }
  static uint32_t ChannelOutageCount () { return s_channelOutages; }

  static void SubscribeChannel (StateCallback cb)
  {
    s_channelListeners.push_back (cb);
  }

  static void SubscribeRoute (StateCallback cb)
  {
    s_routeListeners.push_back (cb);
  }

  static void SetRouteAvailable (bool available, Time when)
  {
    if (s_forceRouteAvailable)
      {
        available = true;
      }

    if (available == s_routeAvailable)
      {
        return;
      }

    s_routeAvailable = available;
    if (available)
      {
        s_lastRouteUp = when;
      }

    for (std::vector<StateCallback>::iterator it = s_routeListeners.begin ();
         it != s_routeListeners.end (); ++it)
      {
        (*it) (available, when);
      }
  }

  static void ScheduleAlternation (Ptr<WifiPhy> phy,
                                   Time start,
                                   Time onDuration,
                                   Time offDuration,
                                   uint32_t cycles,
                                   bool affectPhysicalPhy)
  {
    Time t = start;
    for (uint32_t i = 0; i < cycles; ++i)
      {
        Simulator::Schedule (t,
                             &VtpEnvironment::GoOff,
                             phy,
                             affectPhysicalPhy);
        Simulator::Schedule (t + offDuration,
                             &VtpEnvironment::GoOn,
                             phy,
                             affectPhysicalPhy);
        t += offDuration + onDuration;
      }
  }

private:
  static void GoOff (Ptr<WifiPhy> phy, bool affectPhysicalPhy)
  {
    if (!s_channelAvailable)
      {
        return;
      }

    s_channelAvailable = false;
    s_outageStart = Simulator::Now ();
    ++s_channelOutages;

    // Dans le profil full-independent, le canal est un etat logique
    // applique uniquement au trafic DATA/ACK VTP. Le PHY reste actif afin
    // que les paquets de controle DSR continuent de circuler et que les
    // tentatives TRAD consomment reellement de l'energie radio.
    if (affectPhysicalPhy && phy)
      {
        phy->SetOffMode ();
      }

    for (std::vector<StateCallback>::iterator it = s_channelListeners.begin ();
         it != s_channelListeners.end (); ++it)
      {
        (*it) (false, Simulator::Now ());
      }
  }

  static void GoOn (Ptr<WifiPhy> phy, bool affectPhysicalPhy)
  {
    if (s_channelAvailable)
      {
        return;
      }

    s_channelAvailable = true;
    s_lastChannelUp = Simulator::Now ();
    s_totalChannelOutage += (s_lastChannelUp - s_outageStart);

    if (affectPhysicalPhy && phy)
      {
        phy->ResumeFromOff ();
      }

    for (std::vector<StateCallback>::iterator it = s_channelListeners.begin ();
         it != s_channelListeners.end (); ++it)
      {
        (*it) (true, s_lastChannelUp);
      }
  }

  static bool s_channelAvailable;
  static bool s_routeAvailable;
  static bool s_forceRouteAvailable;
  static Time s_lastChannelUp;
  static Time s_lastRouteUp;
  static Time s_outageStart;
  static Time s_totalChannelOutage;
  static uint32_t s_channelOutages;
  static std::vector<StateCallback> s_channelListeners;
  static std::vector<StateCallback> s_routeListeners;
};

bool VtpEnvironment::s_channelAvailable = true;
bool VtpEnvironment::s_routeAvailable = false;
bool VtpEnvironment::s_forceRouteAvailable = false;
Time VtpEnvironment::s_lastChannelUp = Seconds (0);
Time VtpEnvironment::s_lastRouteUp = Seconds (0);
Time VtpEnvironment::s_outageStart = Seconds (0);
Time VtpEnvironment::s_totalChannelOutage = Seconds (0);
uint32_t VtpEnvironment::s_channelOutages = 0;
std::vector<VtpEnvironment::StateCallback> VtpEnvironment::s_channelListeners;
std::vector<VtpEnvironment::StateCallback> VtpEnvironment::s_routeListeners;

// ===========================================================================
// Statistiques DSR du seul flux etudie
// ===========================================================================

class TopologyStats
{
public:
  void Reset ()
  {
    m_source = Ipv4Address::GetAny ();
    m_destination = Ipv4Address::GetAny ();
    m_enabled = true;
    m_observationStarted = false;
    m_finalized = false;
    m_routeAvailable = false;
    m_noRouteStart = Seconds (0);
    m_totalNoRoute = Seconds (0);
    m_lastTimeout = Seconds (0);
    m_haveLastTimeout = false;
    m_rreqCount = 0;
    m_rreqTimeoutCount = 0;
    m_timerArmedCount = 0;
    m_exactTimerMatchCount = 0;
    m_timerCancelledCount = 0;
    m_timerIncompleteCount = 0;
    m_timerMatchMissCount = 0;
    m_discoverySamplesMs.clear ();
    m_pendingDiscoveries.clear ();
    m_discoveryCompletedCount = 0;
    m_discoveryFailedCount = 0;
    m_discoveryIncompleteCount = 0;
    m_discoveryCompletionMatchMissCount = 0;
    m_retryTimerSamplesMs.clear ();
    m_routeOutagesSec.clear ();
    m_armedTimers.clear ();
  }

  void Configure (Ipv4Address source, Ipv4Address destination)
  {
    m_source = source;
    m_destination = destination;
  }

  void SetEnabled (bool enabled)
  {
    m_enabled = enabled;
  }

  void BeginObservation (bool routeAvailable, Time t)
  {
    if (!m_enabled || m_observationStarted)
      {
        return;
      }

    m_observationStarted = true;
    m_routeAvailable = routeAvailable;
    if (!routeAvailable)
      {
        m_noRouteStart = t;
      }
  }

  void OnRouteState (bool available, Time t)
  {
    if (!m_enabled || m_finalized || !m_observationStarted ||
        available == m_routeAvailable)
      {
        return;
      }

    if (!available)
      {
        m_noRouteStart = t;
      }
    else
      {
        Time duration = t - m_noRouteStart;
        if (duration.IsPositive ())
          {
            m_totalNoRoute += duration;
            m_routeOutagesSec.push_back (duration.GetSeconds ());
          }
      }

    m_routeAvailable = available;
  }

  void OnRreqInit (Ipv4Address source,
                   Ipv4Address destination,
                   uint32_t requestId,
                   Time t)
  {
    if (!m_enabled || m_finalized || source != m_source ||
        destination != m_destination)
      {
        return;
      }

    ++m_rreqCount;
    RreqTimerKey key (source, destination, requestId);
    m_pendingDiscoveries[key] = t;
    m_haveLastTimeout = false;
  }

  void OnRouteDiscoveryCompleted (Ipv4Address source,
                                  Ipv4Address destination,
                                  uint32_t requestId,
                                  Time t)
  {
    if (!m_enabled || m_finalized || source != m_source ||
        destination != m_destination)
      {
        return;
      }

    RreqTimerKey key (source, destination, requestId);
    std::map<RreqTimerKey, Time>::iterator it =
      m_pendingDiscoveries.find (key);
    if (it == m_pendingDiscoveries.end ())
      {
        ++m_discoveryCompletionMatchMissCount;
        return;
      }

    Time duration = t - it->second;
    if (duration.IsPositive () || duration.IsZero ())
      {
        m_discoverySamplesMs.push_back (ToMilliseconds (duration));
        ++m_discoveryCompletedCount;
      }
    m_pendingDiscoveries.erase (it);
    std::map<RreqTimerKey, RreqTimerArm>::iterator timer = m_armedTimers.find (key);
    if (timer != m_armedTimers.end ())
      {
        m_armedTimers.erase (timer);
        ++m_timerCancelledCount;
      }
    m_haveLastTimeout = false;
  }

  void OnRouteDiscoveryFailed (Ipv4Address source,
                               Ipv4Address destination,
                               uint32_t requestId,
                               Time t)
  {
    (void) t;
    if (!m_enabled || m_finalized || source != m_source ||
        destination != m_destination)
      {
        return;
      }

    RreqTimerKey key (source, destination, requestId);
    std::map<RreqTimerKey, Time>::iterator it =
      m_pendingDiscoveries.find (key);
    if (it == m_pendingDiscoveries.end ())
      {
        ++m_discoveryCompletionMatchMissCount;
        return;
      }

    m_pendingDiscoveries.erase (it);
    std::map<RreqTimerKey, RreqTimerArm>::iterator timer = m_armedTimers.find (key);
    if (timer != m_armedTimers.end ())
      {
        m_armedTimers.erase (timer);
        ++m_timerCancelledCount;
      }
    ++m_discoveryFailedCount;
    m_haveLastTimeout = false;
  }

  void OnRreqTimerArmed (Ipv4Address source,
                           Ipv4Address destination,
                           uint32_t requestId,
                           Time retryDelay,
                           Time armedTime)
  {
    if (!m_enabled || m_finalized || source != m_source ||
        destination != m_destination)
      {
        return;
      }

    RreqTimerKey key (source, destination, requestId);
    m_armedTimers[key] = RreqTimerArm (armedTime, retryDelay);
    ++m_timerArmedCount;
  }

  void OnRreqTimeout (Ipv4Address source,
                      Ipv4Address destination,
                      uint32_t requestId,
                      Time t)
  {
    if (!m_enabled || m_finalized || source != m_source ||
        destination != m_destination)
      {
        return;
      }

    ++m_rreqTimeoutCount;

    RreqTimerKey key (source, destination, requestId);
    std::map<RreqTimerKey, RreqTimerArm>::iterator it = m_armedTimers.find (key);
    if (it != m_armedTimers.end ())
      {
        Time elapsed = t - it->second.armedTime;
        if (elapsed.IsPositive () || elapsed.IsZero ())
          {
            m_retryTimerSamplesMs.push_back (ToMilliseconds (elapsed));
            ++m_exactTimerMatchCount;
          }
        m_armedTimers.erase (it);
      }
    else
      {
        // Fallback seulement si la trace d'armement est absente ou si un
        // evenement a ete perdu. Cette valeur n'est pas privilegiee quand
        // une association exacte est disponible.
        ++m_timerMatchMissCount;
        if (m_haveLastTimeout && t >= m_lastTimeout)
          {
            Time interval = t - m_lastTimeout;
            if (interval.IsPositive ())
              {
                m_retryTimerSamplesMs.push_back (ToMilliseconds (interval));
              }
          }
      }

    m_lastTimeout = t;
    m_haveLastTimeout = true;
  }

  void Finalize (Time stop)
  {
    if (!m_enabled || m_finalized)
      {
        return;
      }

    if (m_observationStarted && !m_routeAvailable)
      {
        Time duration = stop - m_noRouteStart;
        if (duration.IsPositive ())
          {
            m_totalNoRoute += duration;
            m_routeOutagesSec.push_back (duration.GetSeconds ());
          }
        m_routeAvailable = true;
      }
    // Les decouvertes encore ouvertes a la fin sont censurees/incompletes.
    // Elles ne contribuent jamais a la moyenne des episodes complets.
    m_discoveryIncompleteCount +=
      static_cast<uint32_t> (m_pendingDiscoveries.size ());
    m_pendingDiscoveries.clear ();
    m_timerIncompleteCount += static_cast<uint32_t> (m_armedTimers.size ());
    m_armedTimers.clear ();
    m_finalized = true;
  }

  // TR utilise dans le modele : armement -> expiration du meme timer RREQ.
  double MeanRetryTimerMs () const { return Mean (m_retryTimerSamplesMs); }
  double StdRetryTimerMs () const { return Std (m_retryTimerSamplesMs); }
  uint32_t RetryTimerSampleCount () const
  {
    return static_cast<uint32_t> (m_retryTimerSamplesMs.size ());
  }

  // Duree complete d'un episode de decouverte, conservee separement.
  double MeanDiscoveryMs () const { return Mean (m_discoverySamplesMs); }
  double StdDiscoveryMs () const { return Std (m_discoverySamplesMs); }
  uint32_t DiscoverySampleCount () const
  {
    return static_cast<uint32_t> (m_discoverySamplesMs.size ());
  }
  uint32_t DiscoveryCompletedCount () const
  {
    return m_discoveryCompletedCount;
  }
  uint32_t DiscoveryFailedCount () const { return m_discoveryFailedCount; }
  uint32_t DiscoveryIncompleteCount () const
  {
    return m_discoveryIncompleteCount;
  }
  uint32_t DiscoveryCompletionMatchMissCount () const
  {
    return m_discoveryCompletionMatchMissCount;
  }

  uint32_t RreqCount () const { return m_rreqCount; }
  uint32_t RreqTimeoutCount () const { return m_rreqTimeoutCount; }
  uint32_t TimerArmedCount () const { return m_timerArmedCount; }
  uint32_t ExactTimerMatchCount () const { return m_exactTimerMatchCount; }
  uint32_t TimerCancelledCount () const { return m_timerCancelledCount; }
  uint32_t TimerIncompleteCount () const { return m_timerIncompleteCount; }
  uint32_t TimerMatchMissCount () const { return m_timerMatchMissCount; }

  double ResetFrequency (double activeDurationSec) const
  {
    return activeDurationSec > 0.0
             ? static_cast<double> (m_rreqTimeoutCount) / activeDurationSec
             : 0.0;
  }

  double PiP (double activeDurationSec) const
  {
    return activeDurationSec > 0.0
             ? m_totalNoRoute.GetSeconds () / activeDurationSec
             : 0.0;
  }

  double MeanRouteOutageMs () const
  {
    std::vector<double> ms;
    for (std::vector<double>::const_iterator it = m_routeOutagesSec.begin ();
         it != m_routeOutagesSec.end (); ++it)
      {
        ms.push_back ((*it) * 1000.0);
      }
    return Mean (ms);
  }

  double RouteResidualMs () const
  {
    double sum = 0.0;
    double sumSquares = 0.0;
    for (std::vector<double>::const_iterator it = m_routeOutagesSec.begin ();
         it != m_routeOutagesSec.end (); ++it)
      {
        sum += *it;
        sumSquares += (*it) * (*it);
      }

    return sum > 0.0 ? 1000.0 * sumSquares / (2.0 * sum) : 0.0;
  }

private:
  static double Mean (const std::vector<double>& values)
  {
    if (values.empty ())
      {
        return 0.0;
      }
    double sum = 0.0;
    for (std::vector<double>::const_iterator it = values.begin ();
         it != values.end (); ++it)
      {
        sum += *it;
      }
    return sum / values.size ();
  }

  static double Std (const std::vector<double>& values)
  {
    if (values.size () < 2)
      {
        return 0.0;
      }
    double mean = Mean (values);
    double sum = 0.0;
    for (std::vector<double>::const_iterator it = values.begin ();
         it != values.end (); ++it)
      {
        double d = *it - mean;
        sum += d * d;
      }
    return std::sqrt (sum / (values.size () - 1));
  }

  struct RreqTimerKey
  {
    RreqTimerKey ()
      : source (Ipv4Address::GetAny ()),
        destination (Ipv4Address::GetAny ()),
        requestId (0)
    {
    }

    RreqTimerKey (Ipv4Address src, Ipv4Address dst, uint32_t id)
      : source (src), destination (dst), requestId (id)
    {
    }

    bool operator< (const RreqTimerKey& other) const
    {
      if (source.Get () != other.source.Get ())
        {
          return source.Get () < other.source.Get ();
        }
      if (destination.Get () != other.destination.Get ())
        {
          return destination.Get () < other.destination.Get ();
        }
      return requestId < other.requestId;
    }

    Ipv4Address source;
    Ipv4Address destination;
    uint32_t requestId;
  };

  struct RreqTimerArm
  {
    RreqTimerArm ()
      : armedTime (Seconds (0)), scheduledDelay (Seconds (0))
    {
    }

    RreqTimerArm (Time armed, Time delay)
      : armedTime (armed), scheduledDelay (delay)
    {
    }

    Time armedTime;
    Time scheduledDelay;
  };

  Ipv4Address m_source;
  Ipv4Address m_destination;
  bool m_enabled = true;
  bool m_observationStarted = false;
  bool m_finalized = false;
  bool m_routeAvailable = false;
  Time m_noRouteStart;
  Time m_totalNoRoute;

  Time m_lastTimeout;
  bool m_haveLastTimeout = false;

  uint32_t m_rreqCount = 0;
  uint32_t m_rreqTimeoutCount = 0;
  uint32_t m_timerArmedCount = 0;
  uint32_t m_exactTimerMatchCount = 0;
  uint32_t m_timerCancelledCount = 0;
  uint32_t m_timerIncompleteCount = 0;
  uint32_t m_timerMatchMissCount = 0;
  std::vector<double> m_discoverySamplesMs;
  std::map<RreqTimerKey, Time> m_pendingDiscoveries;
  uint32_t m_discoveryCompletedCount = 0;
  uint32_t m_discoveryFailedCount = 0;
  uint32_t m_discoveryIncompleteCount = 0;
  uint32_t m_discoveryCompletionMatchMissCount = 0;
  std::vector<double> m_retryTimerSamplesMs;
  std::vector<double> m_routeOutagesSec;
  std::map<RreqTimerKey, RreqTimerArm> m_armedTimers;
};

static TopologyStats g_topology;
static bool g_dsrTraceRreq = false;
static bool g_dsrTraceDiscoveryCompleted = false;
static bool g_dsrTraceDiscoveryFailed = false;
static bool g_dsrTraceTimeout = false;
static bool g_dsrTraceTimerArmed = false;

static void
TraceRreqInit (std::string context,
               Ipv4Address source,
               Ipv4Address destination,
               uint32_t requestId,
               Time t)
{
  (void) context;
  g_topology.OnRreqInit (source, destination, requestId, t);
}

static void
TraceRouteDiscoveryCompleted (std::string context,
                              Ipv4Address source,
                              Ipv4Address destination,
                              uint32_t requestId,
                              Time t)
{
  (void) context;
  g_topology.OnRouteDiscoveryCompleted (source, destination, requestId, t);
}

static void
TraceRouteDiscoveryFailed (std::string context,
                           Ipv4Address source,
                           Ipv4Address destination,
                           uint32_t requestId,
                           Time t)
{
  (void) context;
  g_topology.OnRouteDiscoveryFailed (source, destination, requestId, t);
}

static void
TraceRreqTimerArmed (std::string context,
                     Ipv4Address source,
                     Ipv4Address destination,
                     uint32_t id,
                     Time retryDelay,
                     Time armedTime)
{
  (void) context;
  g_topology.OnRreqTimerArmed (source, destination, id, retryDelay, armedTime);
}

static void
TraceRreqTimeout (std::string context,
                  Ipv4Address source,
                  Ipv4Address destination,
                  uint32_t id,
                  Time t)
{
  (void) context;
  g_topology.OnRreqTimeout (source, destination, id, t);
}

// ===========================================================================
// Recepteur fiable simplifie : DATA/TSN -> ACK
// ===========================================================================

class VtpSink : public Application
{
public:
  static TypeId GetTypeId ()
  {
    static TypeId tid = TypeId ("VtpSink")
      .SetParent<Application> ()
      .AddConstructor<VtpSink> ();
    return tid;
  }

  void Setup (uint16_t port) { m_port = port; }

  uint32_t RxUnique () const { return m_rxUnique; }
  uint32_t Duplicates () const { return m_duplicates; }
  uint32_t ChannelDrops () const { return m_channelDrops; }
  double MeanDelayMs () const { return Mean (m_delaysMs); }
  double TotalDelayMs () const
  {
    double sum = 0.0;
    for (std::vector<double>::const_iterator it = m_delaysMs.begin ();
         it != m_delaysMs.end (); ++it)
      {
        sum += *it;
      }
    return sum;
  }
  double JitterMs () const
  {
    if (m_delaysMs.size () < 2)
      {
        return 0.0;
      }

    double sum = 0.0;
    for (size_t i = 1; i < m_delaysMs.size (); ++i)
      {
        sum += std::fabs (m_delaysMs[i] - m_delaysMs[i - 1]);
      }
    return sum / (m_delaysMs.size () - 1);
  }

private:
  void StartApplication () override
  {
    m_socket = Socket::CreateSocket (
      GetNode (), TypeId::LookupByName ("ns3::UdpSocketFactory"));
    m_socket->Bind (InetSocketAddress (Ipv4Address::GetAny (), m_port));
    m_socket->SetRecvCallback (MakeCallback (&VtpSink::HandleRead, this));
  }

  void StopApplication () override
  {
    if (m_socket)
      {
        m_socket->Close ();
      }
  }

  void HandleRead (Ptr<Socket> socket)
  {
    Ptr<Packet> packet;
    Address from;

    while ((packet = socket->RecvFrom (from)))
      {
        // Canal logique indisponible : le paquet DATA a bien traverse les
        // couches IP/DSR/MAC/PHY, mais le service DATA est considere perdu.
        // Aucun ACK n'est envoye. Les paquets de controle DSR ne passent pas
        // par cette application et restent donc independants de C(t).
        if (!VtpEnvironment::ChannelAvailable ())
          {
            ++m_channelDrops;
            continue;
          }

        SeqTag seq;
        if (!packet->FindFirstMatchingByteTag (seq))
          {
            continue;
          }

        if (m_seen.insert (seq.GetSeq ()).second)
          {
            ++m_rxUnique;
            TimestampTag timestamp;
            if (packet->FindFirstMatchingByteTag (timestamp))
              {
                double delay = ToMilliseconds (Simulator::Now () - timestamp.GetTime ());
                if (delay >= 0.0)
                  {
                    m_delaysMs.push_back (delay);
                  }
              }
          }
        else
          {
            ++m_duplicates;
          }

        Ptr<Packet> ack = Create<Packet> (32);
        SeqTag ackTag;
        ackTag.SetSeq (seq.GetSeq ());
        ack->AddByteTag (ackTag);
        socket->SendTo (ack, 0, from);
      }
  }

  static double Mean (const std::vector<double>& values)
  {
    if (values.empty ())
      {
        return 0.0;
      }

    double sum = 0.0;
    for (std::vector<double>::const_iterator it = values.begin ();
         it != values.end (); ++it)
      {
        sum += *it;
      }
    return sum / values.size ();
  }

  Ptr<Socket> m_socket;
  uint16_t m_port = 9;
  uint32_t m_rxUnique = 0;
  uint32_t m_duplicates = 0;
  uint32_t m_channelDrops = 0;
  std::set<uint32_t> m_seen;
  std::vector<double> m_delaysMs;
};

// ===========================================================================
// Transport SCTP-like unique, politique selectionnable
// ===========================================================================

class VtpSource : public Application
{
public:
  enum Policy
  {
    TRAD,
    VTP1,
    VTP2
  };

  static TypeId GetTypeId ()
  {
    static TypeId tid = TypeId ("VtpSource")
      .SetParent<Application> ()
      .AddConstructor<VtpSource> ();
    return tid;
  }

  void Setup (Address peer,
              Policy policy,
              uint32_t packetSize,
              Time interval,
              Time initialRto,
              Time maximumRto,
              Time lifetime,
              uint32_t maximumAttempts,
              Time routePollInterval,
              Time dsrReleaseGrace,
              Time recoveryPacing,
              Time generationStop,
              Ptr<WifiPhy> phy,
              Ptr<dsr::DsrRouting> dsrRouting,
              bool graphPathDiagnostic,
              Time graphPollInterval,
              const NodeContainer& nodes,
              uint32_t sourceNode,
              uint32_t destinationNode,
              double connectivityRange)
  {
    m_peer = peer;
    m_policy = policy;
    m_packetSize = packetSize;
    m_interval = interval;
    m_initialRto = initialRto;
    m_maximumRto = maximumRto;
    m_lifetime = lifetime;
    m_maximumAttempts = maximumAttempts;
    m_routePollInterval = routePollInterval;
    m_dsrReleaseGrace = dsrReleaseGrace;
    m_recoveryPacing = recoveryPacing;
    m_generationStop = generationStop;
    m_phy = phy;
    m_dsr = dsrRouting;
    m_graphPathDiagnostic = graphPathDiagnostic;
    m_graphPollInterval = graphPollInterval;
    m_nodes = nodes;
    m_sourceNode = sourceNode;
    m_destinationNode = destinationNode;
    m_connectivityRange = connectivityRange;
  }

  uint32_t Generated () const { return m_generated; }
  uint32_t OriginalSubmissions () const { return m_originalSubmissions; }
  uint32_t RetransmissionSubmissions () const { return m_retransmissionSubmissions; }
  uint32_t PhyOriginalAttempts () const { return m_phyOriginalAttempts; }
  uint32_t PhyRetransmissionAttempts () const { return m_phyRetransmissionAttempts; }
  uint32_t PhyLateAttempts () const { return m_phyLateAttempts; }
  uint32_t AvoidedRetransmissions () const { return m_avoidedRetransmissions; }
  uint32_t RtoExpirations () const { return m_rtoExpirations; }
  uint32_t RtoRetransmissions () const { return m_rtoRetransmissions; }
  uint32_t RecoveryRetransmissions () const { return m_recoveryRetransmissions; }
  uint32_t BlockedRtoExpirations () const { return m_blockedRtoExpirations; }
  uint32_t MaxAttemptAbandons () const { return m_maxAttemptAbandons; }
  uint32_t ExpiredWaitingChannel () const { return m_expiredWaitingChannel; }
  uint32_t ExpiredWaitingRoute () const { return m_expiredWaitingRoute; }
  uint32_t ExpiredWaitingBoth () const { return m_expiredWaitingBoth; }
  uint32_t ExpiredInFlight () const { return m_expiredInFlight; }
  uint32_t PendingWaitingChannel () const { return CountPending (true, false); }
  uint32_t PendingWaitingRoute () const { return CountPending (false, true); }
  uint32_t PendingWaitingBoth () const { return CountPending (true, true); }
  uint32_t AttemptsDuringBadChannel () const { return m_attemptsDuringBadChannel; }
  uint32_t AttemptsWithoutRoute () const { return m_attemptsWithoutRoute; }
  uint32_t OriginalsDuringBadChannel () const { return m_originalsDuringBadChannel; }
  uint32_t RetransmissionsDuringBadChannel () const
  { return m_retransmissionsDuringBadChannel; }
  uint32_t OriginalsWithoutRoute () const { return m_originalsWithoutRoute; }
  uint32_t RetransmissionsWithoutRoute () const
  { return m_retransmissionsWithoutRoute; }
  uint32_t OriginalC0P0 () const { return m_originalC0P0; }
  uint32_t OriginalC0P1 () const { return m_originalC0P1; }
  uint32_t OriginalC1P0 () const { return m_originalC1P0; }
  uint32_t OriginalC1P1 () const { return m_originalC1P1; }
  uint32_t RetransmissionC0P0 () const { return m_retransmissionC0P0; }
  uint32_t RetransmissionC0P1 () const { return m_retransmissionC0P1; }
  uint32_t RetransmissionC1P0 () const { return m_retransmissionC1P0; }
  uint32_t RetransmissionC1P1 () const { return m_retransmissionC1P1; }
  uint32_t Acked () const { return m_acked; }
  uint32_t Expired () const { return m_expired; }
  uint32_t PendingCount () const { return static_cast<uint32_t> (m_pending.size ()); }
  uint32_t SocketErrors () const { return m_socketErrors; }
  double MeanRecoveryLatencyMs () const { return MeanOrNaN (m_recoveryLatenciesMs); }
  double StdRecoveryLatencyMs () const { return StdOrNaN (m_recoveryLatenciesMs); }
  double MedianRecoveryLatencyMs () const
  { return PercentileOrNaN (m_recoveryLatenciesMs, 0.50); }
  double P90RecoveryLatencyMs () const
  { return PercentileOrNaN (m_recoveryLatenciesMs, 0.90); }
  double P95RecoveryLatencyMs () const
  { return PercentileOrNaN (m_recoveryLatenciesMs, 0.95); }
  double P99RecoveryLatencyMs () const
  { return PercentileOrNaN (m_recoveryLatenciesMs, 0.99); }
  double MaxRecoveryLatencyMs () const { return MaximumOrNaN (m_recoveryLatenciesMs); }
  double MeanChannelRecoveryLatencyMs () const
  { return MeanOrNaN (m_channelRecoveryLatenciesMs); }
  double MeanRouteRecoveryLatencyMs () const
  { return MeanOrNaN (m_routeRecoveryLatenciesMs); }
  double MeanBothRecoveryLatencyMs () const
  { return MeanOrNaN (m_bothRecoveryLatenciesMs); }
  uint32_t ChannelRecoverySamples () const
  { return static_cast<uint32_t> (m_channelRecoveryLatenciesMs.size ()); }
  uint32_t RouteRecoverySamples () const
  { return static_cast<uint32_t> (m_routeRecoveryLatenciesMs.size ()); }
  uint32_t BothRecoverySamples () const
  { return static_cast<uint32_t> (m_bothRecoveryLatenciesMs.size ()); }
  uint32_t RecoverySampleCount () const
  {
    return static_cast<uint32_t> (m_recoveryLatenciesMs.size ());
  }
  uint32_t GraphPathSamples () const { return m_graphPathSamples; }
  uint32_t CacheGraphTp () const { return m_cacheGraphTp; }
  uint32_t CacheGraphTn () const { return m_cacheGraphTn; }
  uint32_t CacheGraphFp () const { return m_cacheGraphFp; }
  uint32_t CacheGraphFn () const { return m_cacheGraphFn; }
  uint32_t PolicyGraphTp () const { return m_policyGraphTp; }
  uint32_t PolicyGraphTn () const { return m_policyGraphTn; }
  uint32_t PolicyGraphFp () const { return m_policyGraphFp; }
  uint32_t PolicyGraphFn () const { return m_policyGraphFn; }

  double GraphPathAvailableFraction () const
  {
    return Fraction (m_graphPathAvailableSamples, m_graphPathSamples);
  }
  double CacheGraphAgreementFraction () const
  {
    return Fraction (m_cacheGraphTp + m_cacheGraphTn, m_graphPathSamples);
  }
  // Parts de tous les echantillons, conservees pour compatibilite CSV.
  double CacheFalsePositiveFraction () const
  {
    return Fraction (m_cacheGraphFp, m_graphPathSamples);
  }
  double CacheFalseNegativeFraction () const
  {
    return Fraction (m_cacheGraphFn, m_graphPathSamples);
  }
  // Taux conditionnels usuels.
  double CacheFalsePositiveRate () const
  {
    return Fraction (m_cacheGraphFp, m_cacheGraphFp + m_cacheGraphTn);
  }
  double CacheFalseNegativeRate () const
  {
    return Fraction (m_cacheGraphFn, m_cacheGraphFn + m_cacheGraphTp);
  }
  double PolicyGraphAgreementFraction () const
  {
    return Fraction (m_policyGraphTp + m_policyGraphTn, m_graphPathSamples);
  }
  double PolicyFalsePositiveFraction () const
  {
    return Fraction (m_policyGraphFp, m_graphPathSamples);
  }
  double PolicyFalseNegativeFraction () const
  {
    return Fraction (m_policyGraphFn, m_graphPathSamples);
  }
  double PolicyFalsePositiveRate () const
  {
    return Fraction (m_policyGraphFp, m_policyGraphFp + m_policyGraphTn);
  }
  double PolicyFalseNegativeRate () const
  {
    return Fraction (m_policyGraphFn, m_policyGraphFn + m_policyGraphTp);
  }


private:
  enum RecoveryCause
  {
    RECOVERY_NONE = 0,
    RECOVERY_CHANNEL = 1,
    RECOVERY_ROUTE = 2,
    RECOVERY_BOTH = 3
  };

  enum ExpiryReason
  {
    EXPIRY_LIFETIME = 0,
    EXPIRY_MAX_ATTEMPTS = 1
  };

  struct PendingChunk
  {
    Time creationTime;
    Time lastSubmissionTime;
    Time expiryTime;
    Time currentRto;
    uint32_t attempts = 0;
    uint32_t nextAttemptId = 0;
    bool lastSubmissionReachedPhy = false;
    bool waiting = false;
    bool lowerLayerMayHoldPacket = false;
    bool waitingForChannel = false;
    bool waitingForRoute = false;
    bool recoveryArmed = false;
    bool recoveryQueued = false;
    RecoveryCause recoveryCause = RECOVERY_NONE;
    Time favorableTime;
    EventId fallbackEvent;
  };

  void StartApplication () override
  {
    m_running = true;

    m_socket = Socket::CreateSocket (
      GetNode (), TypeId::LookupByName ("ns3::UdpSocketFactory"));
    m_socket->Bind ();
    m_socket->Connect (m_peer);
    m_socket->SetRecvCallback (MakeCallback (&VtpSource::HandleAck, this));

    VtpEnvironment::SubscribeChannel (
      MakeCallback (&VtpSource::OnChannelState, this));
    VtpEnvironment::SubscribeRoute (
      MakeCallback (&VtpSource::OnRouteState, this));

    if (m_phy)
      {
        m_phy->TraceConnectWithoutContext (
          "PhyTxBegin", MakeCallback (&VtpSource::OnPhyTxBegin, this));
      }

    bool initialRoute = PeekRouteAvailable ();
    VtpEnvironment::SetRouteAvailable (initialRoute, Simulator::Now ());
    g_topology.BeginObservation (initialRoute, Simulator::Now ());

    m_generateEvent = Simulator::Schedule (m_interval, &VtpSource::Generate, this);
    m_timeoutEvent = Simulator::Schedule (MilliSeconds (10),
                                          &VtpSource::CheckTimeouts,
                                          this);
    m_routePollEvent = Simulator::Schedule (m_routePollInterval,
                                            &VtpSource::PollRoute,
                                            this);
    if (m_graphPathDiagnostic)
      {
        SampleGraphPath ();
        m_graphPollEvent = Simulator::Schedule (m_graphPollInterval,
                                                &VtpSource::PollGraphPath,
                                                this);
      }
  }

  void StopApplication () override
  {
    m_running = false;

    if (m_generateEvent.IsRunning ())
      {
        Simulator::Cancel (m_generateEvent);
      }
    if (m_timeoutEvent.IsRunning ())
      {
        Simulator::Cancel (m_timeoutEvent);
      }
    if (m_routePollEvent.IsRunning ())
      {
        Simulator::Cancel (m_routePollEvent);
      }
    if (m_graphPollEvent.IsRunning ())
      {
        Simulator::Cancel (m_graphPollEvent);
      }
    if (m_recoveryDrainEvent.IsRunning ())
      {
        Simulator::Cancel (m_recoveryDrainEvent);
      }
    m_recoveryQueue.clear ();

    for (std::map<uint32_t, PendingChunk>::iterator it = m_pending.begin ();
         it != m_pending.end (); ++it)
      {
        if (it->second.fallbackEvent.IsRunning ())
          {
            Simulator::Cancel (it->second.fallbackEvent);
          }
      }

    if (m_socket)
      {
        m_socket->Close ();
      }
  }

  void Generate ()
  {
    if (!m_running || Simulator::Now () >= m_generationStop)
      {
        return;
      }

    uint32_t seq = m_nextSeq++;
    ++m_generated;

    PendingChunk chunk;
    chunk.creationTime = Simulator::Now ();
    chunk.lastSubmissionTime = Simulator::Now ();
    chunk.expiryTime = Simulator::Now () + m_lifetime;
    chunk.currentRto = m_initialRto;
    chunk.attempts = 1;

    std::pair<std::map<uint32_t, PendingChunk>::iterator, bool> inserted =
      m_pending.insert (std::make_pair (seq, chunk));

    SubmitPacket (seq, inserted.first->second, false);
    if (Simulator::Now () + m_interval < m_generationStop)
      {
        m_generateEvent = Simulator::Schedule (m_interval, &VtpSource::Generate, this);
      }
  }

  void CheckTimeouts ()
  {
    if (!m_running)
      {
        return;
      }

    Time now = Simulator::Now ();
    std::vector<std::pair<uint32_t, ExpiryReason> > expired;

    for (std::map<uint32_t, PendingChunk>::iterator it = m_pending.begin ();
         it != m_pending.end (); ++it)
      {
        uint32_t seq = it->first;
        PendingChunk& chunk = it->second;

        if (now >= chunk.expiryTime)
          {
            expired.push_back (std::make_pair (seq, EXPIRY_LIFETIME));
            continue;
          }

        if (chunk.waiting)
          {
            continue;
          }

        if (now - chunk.lastSubmissionTime < chunk.currentRto)
          {
            continue;
          }

        ++m_rtoExpirations;

        if (chunk.attempts >= m_maximumAttempts)
          {
            ++m_maxAttemptAbandons;
            expired.push_back (std::make_pair (seq, EXPIRY_MAX_ATTEMPTS));
            continue;
          }

        if (CanPolicyRetransmit ())
          {
            ++m_rtoRetransmissions;
            Retransmit (seq, chunk, false);
          }
        else
          {
            EnterPersistentWait (chunk);
          }
      }

    for (std::vector<std::pair<uint32_t, ExpiryReason> >::const_iterator it =
           expired.begin (); it != expired.end (); ++it)
      {
        RemoveExpired (it->first, it->second);
      }

    m_timeoutEvent = Simulator::Schedule (MilliSeconds (10),
                                          &VtpSource::CheckTimeouts,
                                          this);
  }

  bool CanPolicyRetransmit () const
  {
    if (m_policy == TRAD)
      {
        return true;
      }
    if (m_policy == VTP1)
      {
        return VtpEnvironment::ChannelAvailable ();
      }
    return VtpEnvironment::ChannelAvailable () &&
           VtpEnvironment::RouteAvailable ();
  }

  void EnterPersistentWait (PendingChunk& chunk)
  {
    if (chunk.waiting)
      {
        return;
      }

    chunk.waiting = true;
    chunk.waitingForChannel = !VtpEnvironment::ChannelAvailable ();
    chunk.waitingForRoute = (m_policy == VTP2) &&
                            !VtpEnvironment::RouteAvailable ();
    ++m_blockedRtoExpirations;
    ++m_avoidedRetransmissions;

    // lastSubmissionTime reste inchange : la reprise est evenementielle.
  }

  void OnChannelState (bool available, Time t)
  {
    if (!m_running)
      {
        return;
      }

    if (!available)
      {
        for (std::map<uint32_t, PendingChunk>::iterator it = m_pending.begin ();
             it != m_pending.end (); ++it)
          {
            if (it->second.waiting)
              {
                it->second.waitingForChannel = true;
              }
          }
        return;
      }

    // ResumeFromOff() et la notification d'environnement peuvent survenir
    // dans le meme evenement. On differe tres legerement la reprise afin que
    // le PHY ait effectivement quitte l'etat OFF avant toute soumission.
    Simulator::Schedule (MicroSeconds (1),
                         &VtpSource::HandleChannelRecovery,
                         this,
                         t);
  }

  void HandleChannelRecovery (Time transitionTime)
  {
    if (!m_running)
      {
        return;
      }

    // Une nouvelle coupure a pu survenir pendant le micro-delai de garde.
    if (!VtpEnvironment::ChannelAvailable ())
      {
        return;
      }

    // transitionTime reste l'instant physique de retour du canal. La latence
    // de reprise inclut donc explicitement le micro-delai de garde.
    ArmRecoveryMeasurements (transitionTime);

    if (m_policy == VTP1 || m_policy == VTP2)
      {
        ResumeWaitingChunks ();
      }
  }

  void OnRouteState (bool available, Time t)
  {
    if (!m_running)
      {
        return;
      }

    if (!available)
      {
        if (m_policy == VTP2)
          {
            for (std::map<uint32_t, PendingChunk>::iterator it = m_pending.begin ();
                 it != m_pending.end (); ++it)
              {
                if (it->second.waiting)
                  {
                    it->second.waitingForRoute = true;
                  }
              }
          }
        return;
      }

    ArmRecoveryMeasurements (t);
    if (m_policy == VTP2)
      {
        ResumeWaitingChunks ();
      }
    else if (m_policy == VTP1)
      {
        ScheduleUnobservedRecoveryFallbacks ();
      }
  }

  void ArmRecoveryMeasurements (Time transitionTime)
  {
    (void) transitionTime;

    if (!CanPolicyRetransmit ())
      {
        return;
      }

    for (std::map<uint32_t, PendingChunk>::iterator it = m_pending.begin ();
         it != m_pending.end (); ++it)
      {
        PendingChunk& chunk = it->second;
        if (!chunk.waiting || chunk.recoveryArmed)
          {
            continue;
          }

        if (chunk.waitingForChannel && chunk.waitingForRoute)
          {
            chunk.recoveryCause = RECOVERY_BOTH;
            chunk.favorableTime = VtpEnvironment::LastChannelUp () >
                                   VtpEnvironment::LastRouteUp ()
                                     ? VtpEnvironment::LastChannelUp ()
                                     : VtpEnvironment::LastRouteUp ();
          }
        else if (chunk.waitingForChannel)
          {
            chunk.recoveryCause = RECOVERY_CHANNEL;
            chunk.favorableTime = VtpEnvironment::LastChannelUp ();
          }
        else if (chunk.waitingForRoute)
          {
            chunk.recoveryCause = RECOVERY_ROUTE;
            chunk.favorableTime = VtpEnvironment::LastRouteUp ();
          }
        else
          {
            continue;
          }

        chunk.recoveryArmed = true;
      }
  }

  void QueueForRecovery (uint32_t seq, PendingChunk& chunk)
  {
    if (!chunk.waiting || chunk.recoveryQueued)
      {
        return;
      }

    if (Simulator::Now () >= chunk.expiryTime)
      {
        return;
      }

    chunk.recoveryQueued = true;
    m_recoveryQueue.push_back (seq);
  }

  void ScheduleRecoveryDrain ()
  {
    if (!m_running || m_recoveryQueue.empty () ||
        m_recoveryDrainEvent.IsRunning () ||
        !CanPolicyRetransmit ())
      {
        return;
      }

    m_recoveryDrainEvent = Simulator::ScheduleNow (
      &VtpSource::DrainRecoveryQueue,
      this);
  }

  void DrainRecoveryQueue ()
  {
    // L'evenement courant est termine; une nouvelle planification pourra
    // etre effectuee ci-dessous ou lors du prochain ChannelUp/RouteUp.
    m_recoveryDrainEvent = EventId ();

    if (!m_running || !CanPolicyRetransmit ())
      {
        return;
      }

    while (!m_recoveryQueue.empty ())
      {
        uint32_t seq = m_recoveryQueue.front ();
        m_recoveryQueue.pop_front ();

        std::map<uint32_t, PendingChunk>::iterator it = m_pending.find (seq);
        if (it == m_pending.end ())
          {
            continue;
          }

        PendingChunk& chunk = it->second;
        chunk.recoveryQueued = false;

        if (!chunk.waiting || Simulator::Now () >= chunk.expiryTime)
          {
            continue;
          }

        if (!CanPolicyRetransmit ())
          {
            QueueForRecovery (seq, chunk);
            return;
          }

        ++m_recoveryRetransmissions;
        Retransmit (seq, chunk, true);
        break; // Au plus une retransmission par pas de cadence.
      }

    if (!m_recoveryQueue.empty () && CanPolicyRetransmit ())
      {
        m_recoveryDrainEvent = Simulator::Schedule (
          m_recoveryPacing,
          &VtpSource::DrainRecoveryQueue,
          this);
      }
  }

  void ResumeWaitingChunks ()
  {
    if (!CanPolicyRetransmit ())
      {
        return;
      }

    for (std::map<uint32_t, PendingChunk>::iterator it = m_pending.begin ();
         it != m_pending.end (); ++it)
      {
        uint32_t seq = it->first;
        PendingChunk& chunk = it->second;

        if (!chunk.waiting || Simulator::Now () >= chunk.expiryTime)
          {
            continue;
          }

        // Si la derniere soumission n'a jamais atteint le PHY, DSR peut
        // encore en conserver une copie. On laisse d'abord le send-buffer
        // inferieur la liberer; le fallback rejoindra ensuite la meme file
        // cadencee que les autres reprises.
        if (!chunk.lastSubmissionReachedPhy)
          {
            chunk.lowerLayerMayHoldPacket = true;
            if (!chunk.fallbackEvent.IsRunning ())
              {
                chunk.fallbackEvent = Simulator::Schedule (
                  m_dsrReleaseGrace,
                  &VtpSource::DsrReleaseFallback,
                  this,
                  seq);
              }
          }
        else
          {
            QueueForRecovery (seq, chunk);
          }
      }

    ScheduleRecoveryDrain ();
  }

  void ScheduleUnobservedRecoveryFallbacks ()
  {
    if (!VtpEnvironment::ChannelAvailable () ||
        !VtpEnvironment::RouteAvailable ())
      {
        return;
      }

    for (std::map<uint32_t, PendingChunk>::iterator it = m_pending.begin ();
         it != m_pending.end (); ++it)
      {
        PendingChunk& chunk = it->second;
        if (!chunk.recoveryArmed || chunk.lastSubmissionReachedPhy ||
            chunk.fallbackEvent.IsRunning ())
          {
            continue;
          }

        chunk.lowerLayerMayHoldPacket = true;
        chunk.fallbackEvent = Simulator::Schedule (
          m_dsrReleaseGrace,
          &VtpSource::DsrReleaseFallback,
          this,
          it->first);
      }
  }

  void DsrReleaseFallback (uint32_t seq)
  {
    std::map<uint32_t, PendingChunk>::iterator it = m_pending.find (seq);
    if (it == m_pending.end ())
      {
        return;
      }

    PendingChunk& chunk = it->second;
    chunk.fallbackEvent = EventId ();

    // Le fallback peut devenir obsolete si un ACK ou un PhyTxBegin a deja
    // traite la reprise. Dans ce cas aucune nouvelle copie ne doit etre soumise.
    if (!chunk.waiting && !chunk.recoveryArmed)
      {
        return;
      }

    if (!CanPolicyRetransmit ())
      {
        // La condition favorable a disparu pendant le delai de grace. Le
        // prochain ChannelUp/RouteUp reprogrammera ce fallback si necessaire.
        return;
      }

    chunk.lowerLayerMayHoldPacket = false;
    QueueForRecovery (seq, chunk);
    ScheduleRecoveryDrain ();
  }

  static double Fraction (uint32_t numerator, uint32_t denominator)
  {
    return denominator > 0
             ? static_cast<double> (numerator) / denominator
             : std::numeric_limits<double>::quiet_NaN ();
  }

  static void UpdateConfusion (bool observed,
                               bool reference,
                               uint32_t& truePositive,
                               uint32_t& trueNegative,
                               uint32_t& falsePositive,
                               uint32_t& falseNegative)
  {
    if (observed && reference)
      {
        ++truePositive;
      }
    else if (!observed && !reference)
      {
        ++trueNegative;
      }
    else if (observed)
      {
        ++falsePositive;
      }
    else
      {
        ++falseNegative;
      }
  }

  bool GraphPathExists () const
  {
    const uint32_t n = m_nodes.GetN ();
    if (n == 0 || m_sourceNode >= n || m_destinationNode >= n ||
        m_connectivityRange <= 0.0)
      {
        return false;
      }
    if (m_sourceNode == m_destinationNode)
      {
        return true;
      }

    std::vector<bool> visited (n, false);
    std::vector<uint32_t> queue;
    queue.reserve (n);
    visited[m_sourceNode] = true;
    queue.push_back (m_sourceNode);

    for (size_t head = 0; head < queue.size (); ++head)
      {
        const uint32_t u = queue[head];
        Ptr<MobilityModel> mobilityU = m_nodes.Get (u)->GetObject<MobilityModel> ();
        if (!mobilityU)
          {
            continue;
          }
        const Vector positionU = mobilityU->GetPosition ();
        for (uint32_t v = 0; v < n; ++v)
          {
            if (visited[v] || v == u)
              {
                continue;
              }
            Ptr<MobilityModel> mobilityV = m_nodes.Get (v)->GetObject<MobilityModel> ();
            if (!mobilityV)
              {
                continue;
              }
            if (CalculateDistance (positionU, mobilityV->GetPosition ()) <=
                m_connectivityRange)
              {
                if (v == m_destinationNode)
                  {
                    return true;
                  }
                visited[v] = true;
                queue.push_back (v);
              }
          }
      }
    return false;
  }

  void SampleGraphPath ()
  {
    const bool cacheRoute = PeekRouteAvailable ();
    const bool policyRoute = VtpEnvironment::RouteAvailable ();
    const bool graphRoute = GraphPathExists ();
    ++m_graphPathSamples;
    if (graphRoute)
      {
        ++m_graphPathAvailableSamples;
      }

    UpdateConfusion (cacheRoute, graphRoute,
                     m_cacheGraphTp, m_cacheGraphTn,
                     m_cacheGraphFp, m_cacheGraphFn);
    UpdateConfusion (policyRoute, graphRoute,
                     m_policyGraphTp, m_policyGraphTn,
                     m_policyGraphFp, m_policyGraphFn);
  }

  void PollGraphPath ()
  {
    if (!m_running || !m_graphPathDiagnostic ||
        Simulator::Now () >= m_generationStop)
      {
        return;
      }
    SampleGraphPath ();
    if (Simulator::Now () + m_graphPollInterval < m_generationStop)
      {
        m_graphPollEvent = Simulator::Schedule (m_graphPollInterval,
                                                &VtpSource::PollGraphPath,
                                                this);
      }
  }

  void PollRoute ()
  {
    if (!m_running)
      {
        return;
      }

    VtpEnvironment::SetRouteAvailable (PeekRouteAvailable (), Simulator::Now ());
    m_routePollEvent = Simulator::Schedule (m_routePollInterval,
                                            &VtpSource::PollRoute,
                                            this);
  }

  bool PeekRouteAvailable () const
  {
    if (VtpEnvironment::ForceRouteAvailable ())
      {
        return true;
      }

    if (!m_dsr)
      {
        return true;
      }

    Ipv4Address destination = InetSocketAddress::ConvertFrom (m_peer).GetIpv4 ();
    dsr::DsrRouteCacheEntry entry;
    return m_dsr->PeekRoute (destination, entry);
  }

  void Retransmit (uint32_t seq, PendingChunk& chunk, bool fromRecovery)
  {
    if (chunk.fallbackEvent.IsRunning ())
      {
        Simulator::Cancel (chunk.fallbackEvent);
      }

    (void) fromRecovery;
    chunk.waiting = false;
    chunk.lowerLayerMayHoldPacket = false;
    ++chunk.attempts;

    SubmitPacket (seq, chunk, true);
    chunk.currentRto = std::min (chunk.currentRto * 2, m_maximumRto);
  }

  void SubmitPacket (uint32_t seq, PendingChunk& chunk, bool retransmission)
  {
    const bool channelAvailable = VtpEnvironment::ChannelAvailable ();
    const bool routeAvailable = VtpEnvironment::RouteAvailable ();

    if (!channelAvailable)
      {
        ++m_attemptsDuringBadChannel;
        if (retransmission)
          {
            ++m_retransmissionsDuringBadChannel;
          }
        else
          {
            ++m_originalsDuringBadChannel;
          }
      }
    if (!routeAvailable)
      {
        ++m_attemptsWithoutRoute;
        if (retransmission)
          {
            ++m_retransmissionsWithoutRoute;
          }
        else
          {
            ++m_originalsWithoutRoute;
          }
      }

    // Exact 2 x 2 state table. These counters avoid double-count ambiguity
    // when C(t)=0 and P(t)=0 simultaneously.
    if (retransmission)
      {
        if (!channelAvailable && !routeAvailable)
          {
            ++m_retransmissionC0P0;
          }
        else if (!channelAvailable && routeAvailable)
          {
            ++m_retransmissionC0P1;
          }
        else if (channelAvailable && !routeAvailable)
          {
            ++m_retransmissionC1P0;
          }
        else
          {
            ++m_retransmissionC1P1;
          }
      }
    else
      {
        if (!channelAvailable && !routeAvailable)
          {
            ++m_originalC0P0;
          }
        else if (!channelAvailable && routeAvailable)
          {
            ++m_originalC0P1;
          }
        else if (channelAvailable && !routeAvailable)
          {
            ++m_originalC1P0;
          }
        else
          {
            ++m_originalC1P1;
          }
      }

    Ptr<Packet> packet = Create<Packet> (m_packetSize);

    SeqTag seqTag;
    seqTag.SetSeq (seq);
    packet->AddByteTag (seqTag);

    TimestampTag timestamp;
    timestamp.SetTime (chunk.creationTime);
    packet->AddByteTag (timestamp);

    ++chunk.nextAttemptId;
    AttemptTag attemptTag;
    attemptTag.Set (chunk.nextAttemptId, retransmission);
    packet->AddByteTag (attemptTag);

    chunk.lastSubmissionTime = Simulator::Now ();
    chunk.lastSubmissionReachedPhy = false;

    int sent = m_socket->Send (packet);
    if (sent < 0)
      {
        ++m_socketErrors;
      }

    if (retransmission)
      {
        ++m_retransmissionSubmissions;
      }
    else
      {
        ++m_originalSubmissions;
      }
  }

  void OnPhyTxBegin (Ptr<const Packet> packet, double txPowerW)
  {
    (void) txPowerW;

    SeqTag seqTag;
    AttemptTag attemptTag;
    if (!packet->FindFirstMatchingByteTag (seqTag) ||
        !packet->FindFirstMatchingByteTag (attemptTag))
      {
        return;
      }

    const std::pair<uint32_t, uint32_t> key (seqTag.GetSeq (),
                                               attemptTag.GetAttemptId ());
    // Deduplication globale par (sequence, tentative transport). Elle reste
    // valide apres retrait du chunk de m_pending et neutralise les retries MAC
    // ainsi que les evenements PHY tardifs d'une ancienne copie.
    if (!m_seenPhyAttempts.insert (key).second)
      {
        return;
      }

    if (attemptTag.IsRetransmission ())
      {
        ++m_phyRetransmissionAttempts;
      }
    else
      {
        ++m_phyOriginalAttempts;
      }

    std::map<uint32_t, PendingChunk>::iterator it = m_pending.find (seqTag.GetSeq ());
    if (it == m_pending.end ())
      {
        ++m_phyLateAttempts;
        return;
      }

    PendingChunk& chunk = it->second;
    chunk.lastSubmissionReachedPhy = true;
    chunk.lastSubmissionTime = Simulator::Now ();

    if (chunk.waiting && chunk.lowerLayerMayHoldPacket)
      {
        // DSR vient de liberer la copie qu'il avait tamponnee.
        chunk.waiting = false;
        chunk.lowerLayerMayHoldPacket = false;
        chunk.recoveryQueued = false;
        if (chunk.fallbackEvent.IsRunning ())
          {
            Simulator::Cancel (chunk.fallbackEvent);
          }
      }

    if (chunk.recoveryArmed)
      {
        double latency = ToMilliseconds (Simulator::Now () - chunk.favorableTime);
        if (latency >= 0.0 && latency < 1e9)
          {
            m_recoveryLatenciesMs.push_back (latency);
            if (chunk.recoveryCause == RECOVERY_CHANNEL)
              {
                m_channelRecoveryLatenciesMs.push_back (latency);
              }
            else if (chunk.recoveryCause == RECOVERY_ROUTE)
              {
                m_routeRecoveryLatenciesMs.push_back (latency);
              }
            else if (chunk.recoveryCause == RECOVERY_BOTH)
              {
                m_bothRecoveryLatenciesMs.push_back (latency);
              }
          }
        chunk.recoveryArmed = false;
        chunk.recoveryCause = RECOVERY_NONE;
        chunk.waitingForChannel = false;
        chunk.waitingForRoute = false;
      }
  }

  void HandleAck (Ptr<Socket> socket)
  {
    Ptr<Packet> packet;
    Address from;

    while ((packet = socket->RecvFrom (from)))
      {
        SeqTag seq;
        if (!packet->FindFirstMatchingByteTag (seq))
          {
            continue;
          }

        std::map<uint32_t, PendingChunk>::iterator it = m_pending.find (seq.GetSeq ());
        if (it == m_pending.end ())
          {
            continue;
          }

        if (it->second.fallbackEvent.IsRunning ())
          {
            Simulator::Cancel (it->second.fallbackEvent);
          }
        m_pending.erase (it);
        ++m_acked;
      }
  }

  void RemoveExpired (uint32_t seq, ExpiryReason reason)
  {
    std::map<uint32_t, PendingChunk>::iterator it = m_pending.find (seq);
    if (it == m_pending.end ())
      {
        return;
      }

    PendingChunk& chunk = it->second;
    if (chunk.fallbackEvent.IsRunning ())
      {
        Simulator::Cancel (chunk.fallbackEvent);
      }

    if (reason == EXPIRY_LIFETIME)
      {
        if (chunk.waitingForChannel && chunk.waitingForRoute)
          {
            ++m_expiredWaitingBoth;
          }
        else if (chunk.waitingForChannel)
          {
            ++m_expiredWaitingChannel;
          }
        else if (chunk.waitingForRoute)
          {
            ++m_expiredWaitingRoute;
          }
        else
          {
            ++m_expiredInFlight;
          }
      }
    else
      {
        ++m_expiredInFlight;
      }

    m_pending.erase (it);
    ++m_expired;
  }

  uint32_t CountPending (bool channel, bool route) const
  {
    uint32_t count = 0;
    for (std::map<uint32_t, PendingChunk>::const_iterator it = m_pending.begin ();
         it != m_pending.end (); ++it)
      {
        if (it->second.waitingForChannel == channel &&
            it->second.waitingForRoute == route)
          {
            ++count;
          }
      }
    return count;
  }

  static double MeanOrNaN (const std::vector<double>& values)
  {
    return values.empty ()
             ? std::numeric_limits<double>::quiet_NaN ()
             : Mean (values);
  }

  static double StdOrNaN (const std::vector<double>& values)
  {
    if (values.empty ())
      {
        return std::numeric_limits<double>::quiet_NaN ();
      }
    return Std (values);
  }

  static double PercentileOrNaN (const std::vector<double>& values, double p)
  {
    return values.empty ()
             ? std::numeric_limits<double>::quiet_NaN ()
             : Percentile (values, p);
  }

  static double MaximumOrNaN (const std::vector<double>& values)
  {
    return values.empty ()
             ? std::numeric_limits<double>::quiet_NaN ()
             : Maximum (values);
  }

  static double Percentile (const std::vector<double>& values, double p)
  {
    if (values.empty ())
      {
        return 0.0;
      }
    std::vector<double> sorted = values;
    std::sort (sorted.begin (), sorted.end ());
    double index = p * static_cast<double> (sorted.size () - 1);
    size_t low = static_cast<size_t> (std::floor (index));
    size_t high = static_cast<size_t> (std::ceil (index));
    if (low == high)
      {
        return sorted[low];
      }
    double fraction = index - static_cast<double> (low);
    return sorted[low] + fraction * (sorted[high] - sorted[low]);
  }

  static double Maximum (const std::vector<double>& values)
  {
    return values.empty () ? 0.0 : *std::max_element (values.begin (), values.end ());
  }

  static double Mean (const std::vector<double>& values)
  {
    if (values.empty ())
      {
        return 0.0;
      }

    double sum = 0.0;
    for (std::vector<double>::const_iterator it = values.begin ();
         it != values.end (); ++it)
      {
        sum += *it;
      }
    return sum / values.size ();
  }

  static double Std (const std::vector<double>& values)
  {
    if (values.size () < 2)
      {
        return 0.0;
      }

    double mean = Mean (values);
    double sum = 0.0;
    for (std::vector<double>::const_iterator it = values.begin ();
         it != values.end (); ++it)
      {
        double d = *it - mean;
        sum += d * d;
      }
    return std::sqrt (sum / (values.size () - 1));
  }

  Ptr<Socket> m_socket;
  Address m_peer;
  Ptr<WifiPhy> m_phy;
  Ptr<dsr::DsrRouting> m_dsr;
  NodeContainer m_nodes;
  uint32_t m_sourceNode = 0;
  uint32_t m_destinationNode = 0;
  double m_connectivityRange = 0.0;
  bool m_graphPathDiagnostic = false;
  Time m_graphPollInterval = MilliSeconds (100);

  EventId m_generateEvent;
  EventId m_timeoutEvent;
  EventId m_routePollEvent;
  EventId m_graphPollEvent;
  EventId m_recoveryDrainEvent;
  bool m_running = false;

  Policy m_policy = VTP1;
  uint32_t m_packetSize = 512;
  Time m_interval = Seconds (0.1);
  Time m_initialRto = Seconds (1.0);
  Time m_maximumRto = Seconds (16.0);
  Time m_lifetime = Seconds (30.0);
  uint32_t m_maximumAttempts = 7;
  Time m_routePollInterval = MilliSeconds (20);
  Time m_dsrReleaseGrace = MilliSeconds (200);
  Time m_recoveryPacing = MilliSeconds (5);
  Time m_generationStop = Seconds (0);
  uint32_t m_nextSeq = 0;

  std::map<uint32_t, PendingChunk> m_pending;
  std::set<std::pair<uint32_t, uint32_t> > m_seenPhyAttempts;
  std::deque<uint32_t> m_recoveryQueue;
  std::vector<double> m_recoveryLatenciesMs;
  std::vector<double> m_channelRecoveryLatenciesMs;
  std::vector<double> m_routeRecoveryLatenciesMs;
  std::vector<double> m_bothRecoveryLatenciesMs;

  uint32_t m_generated = 0;
  uint32_t m_originalSubmissions = 0;
  uint32_t m_retransmissionSubmissions = 0;
  uint32_t m_phyOriginalAttempts = 0;
  uint32_t m_phyRetransmissionAttempts = 0;
  uint32_t m_phyLateAttempts = 0;
  uint32_t m_avoidedRetransmissions = 0;
  uint32_t m_rtoExpirations = 0;
  uint32_t m_rtoRetransmissions = 0;
  uint32_t m_recoveryRetransmissions = 0;
  uint32_t m_blockedRtoExpirations = 0;
  uint32_t m_maxAttemptAbandons = 0;
  uint32_t m_expiredWaitingChannel = 0;
  uint32_t m_expiredWaitingRoute = 0;
  uint32_t m_expiredWaitingBoth = 0;
  uint32_t m_expiredInFlight = 0;
  uint32_t m_attemptsDuringBadChannel = 0;
  uint32_t m_attemptsWithoutRoute = 0;
  uint32_t m_originalsDuringBadChannel = 0;
  uint32_t m_retransmissionsDuringBadChannel = 0;
  uint32_t m_originalsWithoutRoute = 0;
  uint32_t m_retransmissionsWithoutRoute = 0;
  uint32_t m_originalC0P0 = 0;
  uint32_t m_originalC0P1 = 0;
  uint32_t m_originalC1P0 = 0;
  uint32_t m_originalC1P1 = 0;
  uint32_t m_retransmissionC0P0 = 0;
  uint32_t m_retransmissionC0P1 = 0;
  uint32_t m_retransmissionC1P0 = 0;
  uint32_t m_retransmissionC1P1 = 0;
  uint32_t m_graphPathSamples = 0;
  uint32_t m_graphPathAvailableSamples = 0;
  uint32_t m_cacheGraphTp = 0;
  uint32_t m_cacheGraphTn = 0;
  uint32_t m_cacheGraphFp = 0;
  uint32_t m_cacheGraphFn = 0;
  uint32_t m_policyGraphTp = 0;
  uint32_t m_policyGraphTn = 0;
  uint32_t m_policyGraphFp = 0;
  uint32_t m_policyGraphFn = 0;
  uint32_t m_acked = 0;
  uint32_t m_expired = 0;
  uint32_t m_socketErrors = 0;
};

static VtpSource::Policy
ParsePolicy (const std::string& name)
{
  if (name == "TRAD")
    {
      return VtpSource::TRAD;
    }
  if (name == "VTP1")
    {
      return VtpSource::VTP1;
    }
  if (name == "VTP2")
    {
      return VtpSource::VTP2;
    }

  NS_FATAL_ERROR ("Politique inconnue: " << name << " (TRAD, VTP1 ou VTP2)");
  return VtpSource::TRAD;
}

static bool
FileIsEmpty (const std::string& path)
{
  std::ifstream in (path.c_str (), std::ios::binary | std::ios::ate);
  return !in.good () || in.tellg () == 0;
}

static std::string
StableHash64 (const std::string& text)
{
  uint64_t hash = 1469598103934665603ULL;
  for (std::string::const_iterator it = text.begin (); it != text.end (); ++it)
    {
      hash ^= static_cast<uint8_t> (*it);
      hash *= 1099511628211ULL;
    }
  std::ostringstream out;
  out << std::hex << std::setfill ('0') << std::setw (16) << hash;
  return out.str ();
}

static std::string
CanonicalDouble (double value)
{
  std::ostringstream out;
  out << std::setprecision (17) << value;
  return out.str ();
}

int
main (int argc, char* argv[])
{
  std::string policyName = "VTP1";
  uint32_t nNodes = 50;
  double speed = 5.0;
  double pause = 2.0;
  double areaX = 500.0;
  double areaY = 500.0;
  double range = 300.0;
  double simTime = 300.0;
  double appStart = 10.0;
  double appStopMargin = 35.0;
  uint32_t packetSize = 512;
  double interval = 0.1;
  double rto = 1.0;
  double rtoMax = 16.0;
  double lifetime = 30.0;
  uint32_t maxAttempts = 7;
  double routePoll = 0.02;
  bool graphPathDiagnostic = false;
  double graphPoll = 0.10;
  double dsrGrace = 0.20;
  double recoveryPacing = 0.005;
  uint32_t run = 1;
  uint32_t rngSeed = 12345;
  std::string campaignId = "manual";

  double outageStart = 50.0;
  double outagePhase = 0.0;
  double onDuration = 20.0;
  double offDuration = 10.0;
  uint32_t outageCycles = 5;

  std::string output = "vtp-dsr-raw.csv";
  std::string scenario = "custom";
  bool forceRouteAvailable = false;
  bool physicalChannelOutage = true;
  bool verbose = false;

  CommandLine cmd;
  cmd.AddValue ("policy", "TRAD | VTP1 | VTP2", policyName);
  cmd.AddValue ("nodes", "Nombre de noeuds", nNodes);
  cmd.AddValue ("speed", "Vitesse maximale RandomWaypoint (m/s)", speed);
  cmd.AddValue ("pause", "Pause RandomWaypoint (s)", pause);
  cmd.AddValue ("areaX", "Largeur de la zone (m)", areaX);
  cmd.AddValue ("areaY", "Hauteur de la zone (m)", areaY);
  cmd.AddValue ("range", "Portee radio RangePropagationLossModel (m)", range);
  cmd.AddValue ("simTime", "Duree totale de simulation (s)", simTime);
  cmd.AddValue ("appStart", "Debut de l'application (s)", appStart);
  cmd.AddValue ("appStopMargin", "Marge de vidange apres arret de generation (s)", appStopMargin);
  cmd.AddValue ("pktSize", "Taille DATA (octets)", packetSize);
  cmd.AddValue ("interval", "Intervalle de generation (s)", interval);
  cmd.AddValue ("rto", "T3-rtx initial abstrait (s)", rto);
  cmd.AddValue ("rtoMax", "T3-rtx maximal abstrait (s)", rtoMax);
  cmd.AddValue ("lifetime", "Duree de vie absolue d'un chunk (s)", lifetime);
  cmd.AddValue ("maxAttempts", "Nombre maximal de soumissions", maxAttempts);
  cmd.AddValue ("routePoll", "Periode de sondage du cache DSR (s)", routePoll);
  cmd.AddValue ("graphPathDiagnostic",
                "1: echantillonner l'existence d'un chemin topologique selon la portee",
                graphPathDiagnostic);
  cmd.AddValue ("graphPoll",
                "Periode du diagnostic de chemin topologique (s)",
                graphPoll);
  cmd.AddValue ("dsrGrace", "Delai laisse au send-buffer DSR apres RREP (s)", dsrGrace);
  cmd.AddValue ("recoveryPacing",
                "Intervalle entre deux retransmissions de reprise VTP (s)",
                recoveryPacing);
  cmd.AddValue ("run", "Numero de repetition RNG", run);
  cmd.AddValue ("seed", "Graine RNG de base", rngSeed);
  cmd.AddValue ("campaignId", "Identifiant stable de campagne", campaignId);
  cmd.AddValue ("outStart", "Debut de base de la premiere coupure (s)", outageStart);
  cmd.AddValue ("outagePhase",
                "Decalage commun du calendrier de coupures pour ce run (s)",
                outagePhase);
  cmd.AddValue ("onDur", "Duree du canal disponible entre coupures (s)", onDuration);
  cmd.AddValue ("offDur", "Duree d'une coupure (s)", offDuration);
  cmd.AddValue ("cycles", "Nombre de coupures", outageCycles);
  cmd.AddValue ("out", "Fichier CSV brut", output);
  cmd.AddValue ("scenario", "Nom du profil experimental", scenario);
  cmd.AddValue ("forceRouteAvailable",
                "Forcer P(t)=1 et desactiver les statistiques DSR du profil",
                forceRouteAvailable);
  cmd.AddValue ("physicalChannelOutage",
                "1: eteindre le PHY; 0: outage logique DATA avant generation ACK",
                physicalChannelOutage);
  cmd.AddValue ("verbose", "Activer les logs applicatifs", verbose);
  cmd.Parse (argc, argv);

  if (simTime <= appStart + appStopMargin)
    {
      NS_FATAL_ERROR ("simTime doit etre superieur a appStart + appStopMargin");
    }
  if (interval <= 0.0 || rto <= 0.0 || rtoMax < rto || lifetime <= 0.0 ||
      routePoll <= 0.0 || recoveryPacing <= 0.0 || outagePhase < 0.0 ||
      graphPoll <= 0.0 || onDuration < 0.0 || offDuration <= 0.0 ||
      maxAttempts == 0 || nNodes < 2)
    {
      NS_FATAL_ERROR ("Parametres invalides: verifier interval/rto/rtoMax/lifetime/routePoll/"
                      "recoveryPacing/outagePhase/graphPoll/onDur/offDur/maxAttempts/nodes");
    }
  if (campaignId.empty () || campaignId.find (',') != std::string::npos ||
      campaignId.find ('\n') != std::string::npos ||
      scenario.find (',') != std::string::npos || scenario.find ('\n') != std::string::npos)
    {
      NS_FATAL_ERROR ("campaignId et scenario doivent etre non vides et sans virgule/retour ligne");
    }
  if (appStopMargin < lifetime)
    {
      std::cerr << "AVERTISSEMENT: appStopMargin < lifetime; certains chunks peuvent "
                   "rester pending en fin de simulation.\n";
    }

  VtpSource::Policy policy = ParsePolicy (policyName);

  if (verbose)
    {
      LogComponentEnable ("VtpDsrComparison", LOG_LEVEL_INFO);
    }

  RngSeedManager::SetSeed (rngSeed);
  RngSeedManager::SetRun (run);
  VtpEnvironment::Reset ();
  g_topology.Reset ();
  VtpEnvironment::SetForceRouteAvailable (forceRouteAvailable);
  g_topology.SetEnabled (!forceRouteAvailable);

  // Le calendrier est explicite : le script de campagne calcule outagePhase
  // une seule fois par run et transmet la meme valeur aux trois politiques.
  // Aucun decalage implicite ne depend ici du numero de run.
  const double scheduledOutageStart = outageStart + outagePhase;

  std::ostringstream configuration;
  configuration
    << "schema=4.6"
    << "|scenario=" << scenario
    << "|nodes=" << nNodes
    << "|speed=" << CanonicalDouble (speed)
    << "|pause=" << CanonicalDouble (pause)
    << "|areaX=" << CanonicalDouble (areaX)
    << "|areaY=" << CanonicalDouble (areaY)
    << "|range=" << CanonicalDouble (range)
    << "|simTime=" << CanonicalDouble (simTime)
    << "|appStart=" << CanonicalDouble (appStart)
    << "|appStopMargin=" << CanonicalDouble (appStopMargin)
    << "|pktSize=" << packetSize
    << "|interval=" << CanonicalDouble (interval)
    << "|rto=" << CanonicalDouble (rto)
    << "|rtoMax=" << CanonicalDouble (rtoMax)
    << "|lifetime=" << CanonicalDouble (lifetime)
    << "|maxAttempts=" << maxAttempts
    << "|routePoll=" << CanonicalDouble (routePoll)
    << "|dsrGrace=" << CanonicalDouble (dsrGrace)
    << "|recoveryPacing=" << CanonicalDouble (recoveryPacing)
    << "|outStart=" << CanonicalDouble (outageStart)
    << "|onDur=" << CanonicalDouble (onDuration)
    << "|offDur=" << CanonicalDouble (offDuration)
    << "|cycles=" << outageCycles
    << "|forceRoute=" << forceRouteAvailable
    << "|physicalOutage=" << physicalChannelOutage
    << "|graphDiagnostic=" << graphPathDiagnostic
    << "|graphPoll=" << CanonicalDouble (graphPoll);
  const std::string configurationHash = StableHash64 (configuration.str ());

  std::ostringstream schedule;
  schedule << CanonicalDouble (scheduledOutageStart)
           << '|' << CanonicalDouble (onDuration)
           << '|' << CanonicalDouble (offDuration)
           << '|' << outageCycles;
  const std::string outageScheduleHash = StableHash64 (schedule.str ());

  NodeContainer nodes;
  nodes.Create (nNodes);

  Config::SetDefault ("ns3::WifiRemoteStationManager::NonUnicastMode",
                      StringValue ("DsssRate1Mbps"));
  Config::SetDefault ("ns3::WifiRemoteStationManager::RtsCtsThreshold",
                      StringValue ("2200"));
  Config::SetDefault ("ns3::WifiRemoteStationManager::FragmentationThreshold",
                      StringValue ("2200"));

  WifiHelper wifi;
  wifi.SetStandard (WIFI_STANDARD_80211b);
  wifi.SetRemoteStationManager ("ns3::ConstantRateWifiManager",
                                "DataMode", StringValue ("DsssRate11Mbps"),
                                "ControlMode", StringValue ("DsssRate1Mbps"));

  YansWifiChannelHelper channel;
  channel.SetPropagationDelay ("ns3::ConstantSpeedPropagationDelayModel");
  channel.AddPropagationLoss ("ns3::RangePropagationLossModel",
                              "MaxRange", DoubleValue (range));

  YansWifiPhyHelper phyHelper;
  phyHelper.SetChannel (channel.Create ());

  WifiMacHelper mac;
  mac.SetType ("ns3::AdhocWifiMac");
  NetDeviceContainer devices = wifi.Install (phyHelper, mac, nodes);

  BasicEnergySourceHelper energyHelper;
  energyHelper.Set ("BasicEnergySourceInitialEnergyJ", DoubleValue (1000.0));
  EnergySourceContainer energySources = energyHelper.Install (nodes);

  WifiRadioEnergyModelHelper radioEnergy;
  radioEnergy.Set ("TxCurrentA", DoubleValue (0.0174));
  radioEnergy.Set ("RxCurrentA", DoubleValue (0.0197));
  radioEnergy.Set ("IdleCurrentA", DoubleValue (0.0002));
  radioEnergy.Install (devices, energySources);

  MobilityHelper mobility;
  ObjectFactory positionFactory;
  positionFactory.SetTypeId ("ns3::RandomRectanglePositionAllocator");

  std::ostringstream xRv;
  std::ostringstream yRv;
  xRv << "ns3::UniformRandomVariable[Min=0.0|Max=" << areaX << "]";
  yRv << "ns3::UniformRandomVariable[Min=0.0|Max=" << areaY << "]";
  positionFactory.Set ("X", StringValue (xRv.str ()));
  positionFactory.Set ("Y", StringValue (yRv.str ()));

  Ptr<PositionAllocator> positions =
    positionFactory.Create ()->GetObject<PositionAllocator> ();
  mobility.SetPositionAllocator (positions);

  if (speed <= 0.0)
    {
      mobility.SetMobilityModel ("ns3::ConstantPositionMobilityModel");
    }
  else
    {
      std::ostringstream speedRv;
      std::ostringstream pauseRv;
      speedRv << "ns3::UniformRandomVariable[Min=0.1|Max=" << speed << "]";
      pauseRv << "ns3::ConstantRandomVariable[Constant=" << pause << "]";
      mobility.SetMobilityModel ("ns3::RandomWaypointMobilityModel",
                                 "Speed", StringValue (speedRv.str ()),
                                 "Pause", StringValue (pauseRv.str ()),
                                 "PositionAllocator", PointerValue (positions));
    }
  mobility.Install (nodes);

  InternetStackHelper internet;
  DsrMainHelper dsrMain;
  DsrHelper dsr;
  internet.Install (nodes);
  dsrMain.Install (dsr, nodes);

  Ipv4AddressHelper address;
  address.SetBase ("10.1.1.0", "255.255.255.0");
  Ipv4InterfaceContainer interfaces = address.Assign (devices);

  uint32_t sourceNode = 0;
  uint32_t destinationNode = nNodes - 1;
  uint16_t port = 9;
  Ipv4Address sourceAddress = interfaces.GetAddress (sourceNode);
  Ipv4Address destinationAddress = interfaces.GetAddress (destinationNode);
  g_topology.Configure (sourceAddress, destinationAddress);
  VtpEnvironment::SubscribeRoute (
    MakeCallback (&TopologyStats::OnRouteState, &g_topology));

  // Les mesures fR et TR concernent uniquement le flux de la source.
  // Connecter tous les noeuds comptait aussi les timeouts des relais.
  std::ostringstream dsrBase;
  dsrBase << "/NodeList/" << sourceNode << "/$ns3::dsr::DsrRouting/";

  g_dsrTraceRreq = Config::ConnectFailSafe (
    dsrBase.str () + "RreqInit", MakeCallback (&TraceRreqInit));
  g_dsrTraceDiscoveryCompleted = Config::ConnectFailSafe (
    dsrBase.str () + "RouteDiscoveryCompleted",
    MakeCallback (&TraceRouteDiscoveryCompleted));
  g_dsrTraceDiscoveryFailed = Config::ConnectFailSafe (
    dsrBase.str () + "RouteDiscoveryFailed",
    MakeCallback (&TraceRouteDiscoveryFailed));
  g_dsrTraceTimerArmed = Config::ConnectFailSafe (
    dsrBase.str () + "RreqTimerArmed", MakeCallback (&TraceRreqTimerArmed));
  g_dsrTraceTimeout = Config::ConnectFailSafe (
    dsrBase.str () + "RreqTimeout", MakeCallback (&TraceRreqTimeout));

  Ptr<VtpSink> sink = CreateObject<VtpSink> ();
  sink->Setup (port);
  nodes.Get (destinationNode)->AddApplication (sink);
  sink->SetStartTime (Seconds (1.0));
  sink->SetStopTime (Seconds (simTime));

  Ptr<WifiNetDevice> sourceWifi = DynamicCast<WifiNetDevice> (devices.Get (sourceNode));
  Ptr<WifiPhy> sourcePhy = sourceWifi ? sourceWifi->GetPhy () : 0;

  Ptr<dsr::DsrRouting> dsrRouting;
  Ptr<Ipv4L3Protocol> l3 = nodes.Get (sourceNode)->GetObject<Ipv4L3Protocol> ();
  if (l3)
    {
      Ptr<IpL4Protocol> protocol = l3->GetProtocol (dsr::DsrRouting::PROT_NUMBER);
      dsrRouting = DynamicCast<dsr::DsrRouting> (protocol);
    }
  if (!dsrRouting)
    {
      std::cerr << "ERREUR: objet DsrRouting introuvable sur la source.\n";
      return 1;
    }

  double appStop = simTime - appStopMargin;

  Ptr<VtpSource> source = CreateObject<VtpSource> ();
  source->Setup (InetSocketAddress (destinationAddress, port),
                 policy,
                 packetSize,
                 Seconds (interval),
                 Seconds (rto),
                 Seconds (rtoMax),
                 Seconds (lifetime),
                 maxAttempts,
                 Seconds (routePoll),
                 Seconds (dsrGrace),
                 Seconds (recoveryPacing),
                 Seconds (appStop),
                 sourcePhy,
                 dsrRouting,
                 graphPathDiagnostic,
                 Seconds (graphPoll),
                 nodes,
                 sourceNode,
                 destinationNode,
                 range);
  nodes.Get (sourceNode)->AddApplication (source);

  source->SetStartTime (Seconds (appStart));
  source->SetStopTime (Seconds (simTime));

  // Arrete les statistiques topologiques a la fin de la periode de generation,
  // tout en laissant le transport vider sa file jusqu'a simTime.
  Simulator::Schedule (Seconds (appStop), &TopologyStats::Finalize,
                       &g_topology, Seconds (appStop));

  VtpEnvironment::ScheduleAlternation (sourcePhy,
                                       Seconds (scheduledOutageStart),
                                       Seconds (onDuration),
                                       Seconds (offDuration),
                                       outageCycles,
                                       physicalChannelOutage);

  Simulator::Stop (Seconds (simTime));
  Simulator::Run ();

  double activeDuration = appStop - appStart;

  uint32_t received = sink->RxUnique ();
  double lambda = activeDuration > 0.0
                    ? static_cast<double> (source->Generated ()) / activeDuration
                    : 0.0;
  double pdr = source->Generated () > 0
                 ? 100.0 * received / source->Generated ()
                 : 0.0;

  double sourceEnergyJ = energySources.Get (sourceNode)->GetInitialEnergy () -
                         energySources.Get (sourceNode)->GetRemainingEnergy ();
  double networkEnergyJ = 0.0;
  for (uint32_t i = 0; i < energySources.GetN (); ++i)
    {
      networkEnergyJ += energySources.Get (i)->GetInitialEnergy () -
                        energySources.Get (i)->GetRemainingEnergy ();
    }

  double deliveredBits = static_cast<double> (received) * packetSize * 8.0;
  double energyPerDeliveredBitMicroJ = deliveredBits > 0.0
                                         ? networkEnergyJ * 1e6 / deliveredBits
                                         : 0.0;

  double trMs = g_topology.MeanRetryTimerMs ();
  double discoveryMs = g_topology.MeanDiscoveryMs ();
  double fR = g_topology.ResetFrequency (activeDuration);
  double piP = g_topology.PiP (activeDuration);
  double rpMs = g_topology.RouteResidualMs ();
  double pathWaitComponentMs = piP * rpMs;
  double resetCostMs = lambda > 0.0 ? (fR / lambda) * trMs : 0.0;

  uint32_t undelivered = source->Generated () > received
                           ? source->Generated () - received
                           : 0;
  double penalizedDelayMs = source->Generated () > 0
    ? (sink->TotalDelayMs () + undelivered * lifetime * 1000.0) /
        source->Generated ()
    : 0.0;

  int64_t conservationDelta = static_cast<int64_t> (source->Generated ()) -
    static_cast<int64_t> (source->Acked () + source->Expired () +
                          source->PendingCount ());
  int64_t rtoBalanceDelta = static_cast<int64_t> (source->RtoExpirations ()) -
    static_cast<int64_t> (source->RtoRetransmissions () +
                          source->BlockedRtoExpirations () +
                          source->MaxAttemptAbandons ());
  int64_t badChannelSplitDelta =
    static_cast<int64_t> (source->AttemptsDuringBadChannel ()) -
    static_cast<int64_t> (source->OriginalsDuringBadChannel () +
                          source->RetransmissionsDuringBadChannel ());
  int64_t noRouteSplitDelta =
    static_cast<int64_t> (source->AttemptsWithoutRoute ()) -
    static_cast<int64_t> (source->OriginalsWithoutRoute () +
                          source->RetransmissionsWithoutRoute ());
  int64_t originalStateBalanceDelta =
    static_cast<int64_t> (source->OriginalSubmissions ()) -
    static_cast<int64_t> (source->OriginalC0P0 () + source->OriginalC0P1 () +
                          source->OriginalC1P0 () + source->OriginalC1P1 ());
  int64_t retransmissionStateBalanceDelta =
    static_cast<int64_t> (source->RetransmissionSubmissions ()) -
    static_cast<int64_t> (source->RetransmissionC0P0 () +
                          source->RetransmissionC0P1 () +
                          source->RetransmissionC1P0 () +
                          source->RetransmissionC1P1 ());
  int64_t rreqTimerBalanceDelta =
    static_cast<int64_t> (g_topology.TimerArmedCount ()) -
    static_cast<int64_t> (g_topology.ExactTimerMatchCount () +
                          g_topology.TimerCancelledCount () +
                          g_topology.TimerIncompleteCount ());
  int64_t cacheGraphBalanceDelta =
    static_cast<int64_t> (source->GraphPathSamples ()) -
    static_cast<int64_t> (source->CacheGraphTp () + source->CacheGraphTn () +
                          source->CacheGraphFp () + source->CacheGraphFn ());
  int64_t policyGraphBalanceDelta =
    static_cast<int64_t> (source->GraphPathSamples ()) -
    static_cast<int64_t> (source->PolicyGraphTp () + source->PolicyGraphTn () +
                          source->PolicyGraphFp () + source->PolicyGraphFn ());

  bool empty = FileIsEmpty (output);
  std::ofstream csv (output.c_str (), std::ios::app);
  csv << std::fixed << std::setprecision (9);

  if (empty)
    {
      csv << "schema_version,campaign_id,configuration_hash,outage_schedule_hash,"
          << "scenario,policy,speed,run,rng_seed,rng_run,nodes,pause_s,area_x_m,area_y_m,range_m,"
          << "sim_time_s,app_start_s,generation_stop_s,app_stop_margin_s,packet_size_bytes,"
          << "generation_interval_s,rto_initial_s,rto_max_s,lifetime_s,max_attempts,"
          << "route_poll_s,dsr_grace_s,force_route_available,route_cache_probe_mode,"
          << "active_duration_s,recovery_pacing_s,lambda_generated_pps,"
          << "outage_base_start_s,outage_phase_s,outage_first_start_s,outage_on_duration_s,"
          << "outage_off_duration_s,outage_cycles_requested,"
          << "graph_path_diagnostic_enabled,graph_path_poll_s,graph_observation_start_s,"
          << "graph_observation_stop_s,route_observation_start_s,route_observation_stop_s,"
          << "graph_path_samples,graph_path_available_fraction,"
          << "cache_graph_tp,cache_graph_tn,cache_graph_fp,cache_graph_fn,"
          << "cache_graph_agreement_fraction,cache_false_positive_fraction,"
          << "cache_false_negative_fraction,cache_false_positive_rate,cache_false_negative_rate,"
          << "policy_graph_tp,policy_graph_tn,policy_graph_fp,policy_graph_fn,"
          << "policy_graph_agreement_fraction,policy_false_positive_fraction,"
          << "policy_false_negative_fraction,policy_false_positive_rate,policy_false_negative_rate,"
          << "cache_graph_balance_delta,policy_graph_balance_delta,"
          << "generated,tx_original_submissions,tx_retrans_submissions,"
          << "rto_expirations,rto_retransmissions,recovery_retransmissions,"
          << "blocked_rto_expirations,max_attempt_abandons,"
          << "phy_original_attempts,phy_retrans_attempts,phy_late_attempts,avoided_retransmissions,"
          << "attempts_bad_channel,originals_bad_channel,retransmissions_bad_channel,"
          << "attempts_without_route,originals_without_route,retransmissions_without_route,"
          << "original_c0_p0,original_c0_p1,original_c1_p0,original_c1_p1,"
          << "retransmission_c0_p0,retransmission_c0_p1,retransmission_c1_p0,retransmission_c1_p1,"
          << "bad_channel_split_delta,no_route_split_delta,original_state_balance_delta,"
          << "retransmission_state_balance_delta,acked,rx_unique,duplicates,"
          << "sink_channel_drops,physical_channel_outage,"
          << "expired,expired_wait_channel,expired_wait_route,expired_wait_both,"
          << "expired_in_flight,pending_end,pending_wait_channel,pending_wait_route,"
          << "pending_wait_both,socket_errors,conservation_delta,rto_balance_delta,"
          << "pdr_pct,e2e_delay_ms,penalized_delay_ms,jitter_ms,"
          << "recovery_latency_ms,recovery_latency_sd_ms,recovery_latency_median_ms,"
          << "recovery_latency_p90_ms,recovery_latency_p95_ms,recovery_latency_p99_ms,"
          << "recovery_latency_max_ms,recovery_samples,"
          << "channel_recovery_ms,channel_recovery_samples,"
          << "route_recovery_ms,route_recovery_samples,both_recovery_ms,both_recovery_samples,"
          << "source_energy_j,network_energy_j,energy_uj_per_delivered_bit,"
          << "channel_outages,channel_outage_time_s,rreq_count,rreq_timeout_count,"
          << "TR_ms,TR_sd_ms,TR_samples,discovery_episode_ms,discovery_episode_sd_ms,"
          << "discovery_episode_samples,discovery_completed_count,discovery_failed_count,"
          << "discovery_incomplete_count,discovery_completion_match_misses,"
          << "fR_per_s,pi_p,route_mean_outage_ms,Rp_ms,"
          << "path_wait_component_ms,reset_cost_ms,rreq_timer_armed_count,"
          << "rreq_timer_exact_matches,rreq_timer_cancelled_count,"
          << "rreq_timer_incomplete_count,rreq_timer_balance_delta,rreq_timer_match_misses,"
          << "dsr_trace_rreq,dsr_trace_discovery_completed,"
          << "dsr_trace_discovery_failed,dsr_trace_timer_armed,dsr_trace_timeout\n";
    }

  csv << "4.6" << ',' << campaignId << ',' << configurationHash << ','
      << outageScheduleHash << ',' << scenario << ',' << policyName << ','
      << speed << ',' << run << ',' << rngSeed << ',' << run << ','
      << nNodes << ',' << pause << ',' << areaX << ',' << areaY << ',' << range << ','
      << simTime << ',' << appStart << ',' << appStop << ',' << appStopMargin << ','
      << packetSize << ',' << interval << ',' << rto << ',' << rtoMax << ','
      << lifetime << ',' << maxAttempts << ',' << routePoll << ',' << dsrGrace << ','
      << (forceRouteAvailable ? 1 : 0) << ',' << "peek-nonmutating" << ','
      << activeDuration << ',' << recoveryPacing << ',' << lambda << ','
      << outageStart << ',' << outagePhase << ',' << scheduledOutageStart << ','
      << onDuration << ',' << offDuration << ',' << outageCycles << ','
      << (graphPathDiagnostic ? 1 : 0) << ',' << graphPoll << ','
      << appStart << ',' << appStop << ',' << appStart << ',' << appStop << ','
      << source->GraphPathSamples () << ','
      << source->GraphPathAvailableFraction () << ','
      << source->CacheGraphTp () << ',' << source->CacheGraphTn () << ','
      << source->CacheGraphFp () << ',' << source->CacheGraphFn () << ','
      << source->CacheGraphAgreementFraction () << ','
      << source->CacheFalsePositiveFraction () << ','
      << source->CacheFalseNegativeFraction () << ','
      << source->CacheFalsePositiveRate () << ','
      << source->CacheFalseNegativeRate () << ','
      << source->PolicyGraphTp () << ',' << source->PolicyGraphTn () << ','
      << source->PolicyGraphFp () << ',' << source->PolicyGraphFn () << ','
      << source->PolicyGraphAgreementFraction () << ','
      << source->PolicyFalsePositiveFraction () << ','
      << source->PolicyFalseNegativeFraction () << ','
      << source->PolicyFalsePositiveRate () << ','
      << source->PolicyFalseNegativeRate () << ','
      << cacheGraphBalanceDelta << ',' << policyGraphBalanceDelta << ','
      << source->Generated () << ','
      << source->OriginalSubmissions () << ','
      << source->RetransmissionSubmissions () << ','
      << source->RtoExpirations () << ','
      << source->RtoRetransmissions () << ','
      << source->RecoveryRetransmissions () << ','
      << source->BlockedRtoExpirations () << ','
      << source->MaxAttemptAbandons () << ','
      << source->PhyOriginalAttempts () << ','
      << source->PhyRetransmissionAttempts () << ','
      << source->PhyLateAttempts () << ','
      << source->AvoidedRetransmissions () << ','
      << source->AttemptsDuringBadChannel () << ','
      << source->OriginalsDuringBadChannel () << ','
      << source->RetransmissionsDuringBadChannel () << ','
      << source->AttemptsWithoutRoute () << ','
      << source->OriginalsWithoutRoute () << ','
      << source->RetransmissionsWithoutRoute () << ','
      << source->OriginalC0P0 () << ',' << source->OriginalC0P1 () << ','
      << source->OriginalC1P0 () << ',' << source->OriginalC1P1 () << ','
      << source->RetransmissionC0P0 () << ','
      << source->RetransmissionC0P1 () << ','
      << source->RetransmissionC1P0 () << ','
      << source->RetransmissionC1P1 () << ','
      << badChannelSplitDelta << ',' << noRouteSplitDelta << ','
      << originalStateBalanceDelta << ',' << retransmissionStateBalanceDelta << ','
      << source->Acked () << ',' << received << ',' << sink->Duplicates () << ','
      << sink->ChannelDrops () << ',' << (physicalChannelOutage ? 1 : 0) << ','
      << source->Expired () << ','
      << source->ExpiredWaitingChannel () << ','
      << source->ExpiredWaitingRoute () << ','
      << source->ExpiredWaitingBoth () << ','
      << source->ExpiredInFlight () << ','
      << source->PendingCount () << ','
      << source->PendingWaitingChannel () << ','
      << source->PendingWaitingRoute () << ','
      << source->PendingWaitingBoth () << ','
      << source->SocketErrors () << ',' << conservationDelta << ','
      << rtoBalanceDelta << ',' << pdr << ','
      << sink->MeanDelayMs () << ',' << penalizedDelayMs << ','
      << sink->JitterMs () << ','
      << source->MeanRecoveryLatencyMs () << ','
      << source->StdRecoveryLatencyMs () << ','
      << source->MedianRecoveryLatencyMs () << ','
      << source->P90RecoveryLatencyMs () << ','
      << source->P95RecoveryLatencyMs () << ','
      << source->P99RecoveryLatencyMs () << ','
      << source->MaxRecoveryLatencyMs () << ','
      << source->RecoverySampleCount () << ','
      << source->MeanChannelRecoveryLatencyMs () << ','
      << source->ChannelRecoverySamples () << ','
      << source->MeanRouteRecoveryLatencyMs () << ','
      << source->RouteRecoverySamples () << ','
      << source->MeanBothRecoveryLatencyMs () << ','
      << source->BothRecoverySamples () << ','
      << sourceEnergyJ << ',' << networkEnergyJ << ','
      << energyPerDeliveredBitMicroJ << ','
      << VtpEnvironment::ChannelOutageCount () << ','
      << VtpEnvironment::TotalChannelOutage ().GetSeconds () << ','
      << g_topology.RreqCount () << ',' << g_topology.RreqTimeoutCount () << ','
      << trMs << ',' << g_topology.StdRetryTimerMs () << ','
      << g_topology.RetryTimerSampleCount () << ','
      << discoveryMs << ',' << g_topology.StdDiscoveryMs () << ','
      << g_topology.DiscoverySampleCount () << ','
      << g_topology.DiscoveryCompletedCount () << ','
      << g_topology.DiscoveryFailedCount () << ','
      << g_topology.DiscoveryIncompleteCount () << ','
      << g_topology.DiscoveryCompletionMatchMissCount () << ','
      << fR << ',' << piP << ','
      << g_topology.MeanRouteOutageMs () << ',' << rpMs << ','
      << pathWaitComponentMs << ',' << resetCostMs << ','
      << g_topology.TimerArmedCount () << ','
      << g_topology.ExactTimerMatchCount () << ','
      << g_topology.TimerCancelledCount () << ','
      << g_topology.TimerIncompleteCount () << ','
      << rreqTimerBalanceDelta << ','
      << g_topology.TimerMatchMissCount () << ','
      << (g_dsrTraceRreq ? 1 : 0) << ','
      << (g_dsrTraceDiscoveryCompleted ? 1 : 0) << ','
      << (g_dsrTraceDiscoveryFailed ? 1 : 0) << ','
      << (g_dsrTraceTimerArmed ? 1 : 0) << ','
      << (g_dsrTraceTimeout ? 1 : 0) << '\n';
  csv.close ();

  std::cout << "\n=== VTP/DSR: " << policyName
            << ", vitesse_max=" << speed << " m/s, run=" << run << " ===\n"
            << "Donnees generees          : " << source->Generated () << "\n"
            << "Recues uniques             : " << received << "\n"
            << "PDR                        : " << pdr << " %\n"
            << "Latence bout-en-bout       : " << sink->MeanDelayMs () << " ms\n"
            << "Latence penalisee pertes   : " << penalizedDelayMs << " ms\n"
            << "Latence de reprise PHY     : ";
  if (source->RecoverySampleCount () == 0)
    {
      std::cout << "NA (0 echantillon)\n";
    }
  else
    {
      std::cout << source->MeanRecoveryLatencyMs () << " +/- "
                << source->StdRecoveryLatencyMs () << " ms ("
                << source->RecoverySampleCount () << " echantillons; mediane="
                << source->MedianRecoveryLatencyMs () << " ms; p95="
                << source->P95RecoveryLatencyMs () << " ms)\n";
    }

  std::cout
            << "Paquets DATA rejetes C(t) : " << sink->ChannelDrops () << "\n"
            << "Coupure physique du PHY    : "
            << (physicalChannelOutage ? "oui" : "non") << "\n"
            << "Cadencement reprise        : "
            << recoveryPacing << " s (" << recoveryPacing * 1000.0 << " ms)\n"
            << "Phase calendrier outages   : " << outagePhase
            << " s; premier debut=" << scheduledOutageStart << " s\n"
            << "Diagnostic chemin graphe   : "
            << (graphPathDiagnostic ? "oui" : "non")
            << " (" << source->GraphPathSamples () << " echantillons)\n"
            << "Retransmissions soumises   : "
            << source->RetransmissionSubmissions () << "\n"
            << "Retransmissions evitees    : "
            << source->AvoidedRetransmissions () << "\n"
            << "RTO expires/direct/bloques : " << source->RtoExpirations () << "/"
            << source->RtoRetransmissions () << "/"
            << source->BlockedRtoExpirations () << "\n"
            << "Expirations canal/route/2  : "
            << source->ExpiredWaitingChannel () << "/"
            << source->ExpiredWaitingRoute () << "/"
            << source->ExpiredWaitingBoth () << "\n"
            << "Soumissions C=0 orig/retr  : "
            << source->OriginalsDuringBadChannel () << "/"
            << source->RetransmissionsDuringBadChannel () << "\n"
            << "Soumissions P=0 orig/retr  : "
            << source->OriginalsWithoutRoute () << "/"
            << source->RetransmissionsWithoutRoute () << "\n"
            << "TR armement->expiration   : " << trMs << " ms ("
            << g_topology.RetryTimerSampleCount () << " echantillons)\n"
            << "Episode decouverte complet : " << discoveryMs << " ms ("
            << g_topology.DiscoverySampleCount () << " echantillons apparies)\n"
            << "Decouvertes ok/echec/incompl.: "
            << g_topology.DiscoveryCompletedCount () << "/"
            << g_topology.DiscoveryFailedCount () << "/"
            << g_topology.DiscoveryIncompleteCount () << "\n"
            << "Appariements fin manques    : "
            << g_topology.DiscoveryCompletionMatchMissCount () << "\n"
            << "fR                         : " << fR << " /s\n"
            << "pi_p                       : " << piP << "\n"
            << "Rp residuel                : " << rpMs << " ms\n"
            << "pi_p * Rp                  : " << pathWaitComponentMs << " ms\n"
            << "(fR/lambda) * TR           : " << resetCostMs << " ms\n"
            << "Energie reseau             : " << networkEnergyJ << " J\n"
            << "Energie/bit livre          : " << energyPerDeliveredBitMicroJ << " uJ/bit\n"
            << "Delta conservation/RTO     : " << conservationDelta << "/"
            << rtoBalanceDelta << "\n"
            << "Delta splits C/P, etats O/R: "
            << badChannelSplitDelta << "/" << noRouteSplitDelta << "/"
            << originalStateBalanceDelta << "/"
            << retransmissionStateBalanceDelta << "\n"
            << "CSV                        : " << output << "\n";

  if (!(g_dsrTraceRreq && g_dsrTraceDiscoveryCompleted &&
        g_dsrTraceDiscoveryFailed && g_dsrTraceTimerArmed &&
        g_dsrTraceTimeout))
    {
      std::cout << "\nAVERTISSEMENT: les traces DSR personnalisees ne sont pas toutes "
                   "connectees.\n"
                << "Le sondage du cache fournit P(t), mais TR, fR ou les episodes peuvent rester incomplets.\n";
    }

  if (g_topology.TimerMatchMissCount () > 0)
    {
      std::cout << "AVERTISSEMENT: " << g_topology.TimerMatchMissCount ()
                << " timeout(s) RREQ sans armement associe.\n";
    }


  if (g_topology.DiscoveryCompletionMatchMissCount () > 0)
    {
      std::cout << "AVERTISSEMENT: "
                << g_topology.DiscoveryCompletionMatchMissCount ()
                << " fin(s) de decouverte sans debut exactement apparie.\n";
    }

  Simulator::Destroy ();
  return 0;
}
