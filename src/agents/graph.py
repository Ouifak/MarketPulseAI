"""
Système multi-agents :
- Un Supervisor route la question vers l'agent pertinent
  (Insight, Anomaly, Forecast)
- Chaque agent possède ses propres outils
- LLM local : Ollama / Qwen3 8B
"""

from typing import Literal, TypedDict, Annotated
import operator

from langchain_ollama import ChatOllama
from langchain.agents import create_agent
from langgraph.graph import StateGraph, END

from agents.tools import (
    get_stock_metrics,
    detect_anomalies,
    get_5min_trend
)


# ============================================================
# 1. LLM LOCAL — OLLAMA
# ============================================================

llm = ChatOllama(
    model="qwen3:8b",
    temperature=0
)


# ============================================================
# 2. STATE DU GRAPHE
# ============================================================

class AgentState(TypedDict):
    messages: Annotated[list, operator.add]
    next_agent: str


# ============================================================
# 3. AGENT INSIGHT
# ============================================================

insight_agent = create_agent(
    model=llm,
    tools=[
        get_stock_metrics,
        get_5min_trend
    ],
    system_prompt=(
        "Tu es un analyste financier. "
        "Utilise les outils disponibles pour récupérer les métriques "
        "de marché nécessaires. "
        "Résume les tendances de prix et de volume à partir des "
        "données Gold. "
        "Réponds en français, de façon concise et claire."
    )
)


# ============================================================
# 4. AGENT ANOMALY
# ============================================================

anomaly_agent = create_agent(
    model=llm,
    tools=[
        detect_anomalies
    ],
    system_prompt=(
        "Tu es un analyste spécialisé dans la détection des "
        "mouvements de marché anormaux. "
        "Utilise l'outil de détection d'anomalies disponible. "
        "Explique clairement pourquoi un symbole est signalé "
        "comme anormal. "
        "Réponds en français et reste factuel."
    )
)


# ============================================================
# 5. AGENT FORECAST
# ============================================================

forecast_agent = create_agent(
    model=llm,
    tools=[
        get_stock_metrics,
        get_5min_trend
    ],
    system_prompt=(
        "Tu analyses la tendance récente d'un symbole afin de "
        "fournir une lecture prospective à très court terme. "
        "Utilise les outils disponibles pour consulter les "
        "métriques et la tendance récente. "
        "Ne garantis jamais une prédiction. "
        "Explique clairement l'incertitude. "
        "Réponds en français."
    )
)


# ============================================================
# 6. SUPERVISOR
# ============================================================

def supervisor_node(state: AgentState) -> AgentState:

    last_message = state["messages"][-1]

    # Gestion des différents formats de messages
    if hasattr(last_message, "content"):
        question = last_message.content
    elif isinstance(last_message, dict):
        question = last_message.get("content", "")
    else:
        question = str(last_message)

    routing_prompt = f"""
Classe cette question dans UNE seule catégorie :

INSIGHT :
- résumé de tendance
- prix
- volume
- VWAP
- métriques générales

ANOMALY :
- détection de mouvements anormaux
- volatilité excessive
- comportement inhabituel
- anomalie

FORECAST :
- tendance à court terme
- évolution possible
- ce qui pourrait se passer ensuite

Question :
{question}

Réponds uniquement par un mot :
INSIGHT
ANOMALY
ou
FORECAST
"""

    decision = llm.invoke(routing_prompt)

    # ChatOllama retourne généralement un AIMessage
    decision_text = decision.content.strip().upper()

    # Nettoyage au cas où le modèle ajoute du texte
    if "ANOMALY" in decision_text:
        next_agent = "ANOMALY"

    elif "FORECAST" in decision_text:
        next_agent = "FORECAST"

    elif "INSIGHT" in decision_text:
        next_agent = "INSIGHT"

    else:
        # Agent par défaut
        next_agent = "INSIGHT"

    return {
        "messages": [],
        "next_agent": next_agent
    }


# ============================================================
# 7. ROUTAGE
# ============================================================

def route(
    state: AgentState
) -> Literal["insight", "anomaly", "forecast"]:

    return state["next_agent"].lower()


# ============================================================
# 8. CONSTRUCTION DU GRAPHE
# ============================================================

graph = StateGraph(AgentState)


# Ajout du Supervisor
graph.add_node(
    "supervisor",
    supervisor_node
)


# Ajout des agents
graph.add_node(
    "insight",
    insight_agent
)

graph.add_node(
    "anomaly",
    anomaly_agent
)

graph.add_node(
    "forecast",
    forecast_agent
)


# Point de départ
graph.set_entry_point("supervisor")


# Routage Supervisor → Agent
graph.add_conditional_edges(
    "supervisor",
    route,
    {
        "insight": "insight",
        "anomaly": "anomaly",
        "forecast": "forecast"
    }
)


# Chaque agent termine le graphe
graph.add_edge(
    "insight",
    END
)

graph.add_edge(
    "anomaly",
    END
)

graph.add_edge(
    "forecast",
    END
)


# Compilation
app_graph = graph.compile()


# ============================================================
# 9. FONCTION PRINCIPALE
# ============================================================

def ask_agents(question: str) -> str:

    result = app_graph.invoke(
        {
            "messages": [
                {
                    "role": "user",
                    "content": question
                }
            ],
            "next_agent": ""
        }
    )

    final_message = result["messages"][-1]

    if hasattr(final_message, "content"):
        return final_message.content

    if isinstance(final_message, dict):
        return final_message.get("content", "")

    return str(final_message)


# ============================================================
# 10. TEST
# ============================================================

if __name__ == "__main__":

    question = input("\n💬 Pose ta question : ")

    print("\n🤖 Analyse en cours...\n")

    response = ask_agents(question)

    print("────────────────────────────────────")
    print("Réponse :")
    print(response)
    print("────────────────────────────────────")