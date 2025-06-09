# Memory Bank Implementation Guide

## Enhanced Memory Bank Structure
```mermaid
flowchart TD
    PB[projectbrief.md] --> PC[productContext.md]
    PB --> SP[systemPatterns.md]
    PB --> TC[techContext.md]
    PC --> AC[activeContext.md]
    SP --> AC
    TC --> AC
    AC --> P[progress.md]
```

### Core File Enhancements
1. **activeContext.md**
   - Added anomaly detection patterns
   - Performance profiling data storage
   - Debug context expansion
   
2. **systemPatterns.md**
   - Cross-mode coordination protocols
   - Performance optimization blueprints
   - Anomaly detection frameworks

## Mode-Specific Workflows

### Code Mode Implementation
```mermaid
flowchart TD
    Start[Start] --> Read[Read Memory Bank]
    Read --> Analyze[Analyze Context]
    Analyze --> Plan[Plan Implementation]
    Plan --> Write[Write Code]
    Write --> Test[Test Changes]
    Test --> Update[Update Memory Bank]
    Update --> Commit[Commit Changes]
```

### Orchestrator Mode Coordination
```mermaid
flowchart TD
    Start[Start] --> Scan[Scan Memory Bank]
    Scan --> Assess[Assess Task Complexity]
    Assess --> Delegate[Delegate to Mode]
    Delegate --> Monitor[Monitor Progress]
    Monitor --> Synthesize[Synthesize Results]
    Synthesize --> Archive[Archive to Memory Bank]
```

### Architect Mode Design Process
```mermaid
flowchart TD
    Start[Start] --> Review[Review Memory Bank]
    Review --> Diagnose[Diagnose System Needs]
    Diagnose --> Design[Design Solution]
    Design --> Document[Document Patterns]
    Document --> Validate[Validate with Code Mode]
    Validate --> Integrate[Integrate Feedback]
```

### Ask Mode Knowledge Flow
```mermaid
flowchart TD
    Start[Start] --> Query[Query Memory Bank]
    Query --> Research[Research Context]
    Research --> Formulate[Formulate Answer]
    Formulate --> CrossCheck[Cross-Check Sources]
    CrossCheck --> Contribute[Contribute Insights]
```

### Debug Mode Analysis
```mermaid
flowchart TD
    Start[Start] --> Inspect[Inspect Memory Bank]
    Inspect --> Reproduce[Reproduce Issue]
    Reproduce --> Diagnose[Diagnose Root Cause]
    Diagnose --> Fix[Implement Fix]
    Fix --> Verify[Verify Solution]
    Verify --> Annotate[Annotate Progress.md]
```

## Cross-Mode Coordination System
```mermaid
flowchart LR
    A[Architect] <--> |"Design Patterns<br>System Blueprints<br>Constraints"| M[Memory Bank]
    C[Code] <--> |"Implementation Details<br>API Contracts<br>Test Cases"| M
    D[Debug] <--> |"Solutions<br>Anomaly Patterns<br>Diagnostic Data<br>Failure Scenarios<br>Root Cause Analysis<br>Workarounds<br>Regression Tests<br>Performance Metrics<br>Resource Utilization<br>Latency Data<br>Error Rates<br>CPU Profiling<br>Memory Profiles<br>Execution Hotspots<br>Call Stack Analysis"| M
    O[Orchestrator] <--> |"Coordination State<br>Task Dependencies<br>Resource Allocation"| M
    Q[Ask] <--> |"Knowledge Base<br>Research Findings<br>Documentation Links"| M
    
    subgraph Mode Interactions
        O --> |Deploys Tasks| C
        O --> |Requests Analysis| A
        O --> |Flags Issues| D
        O --> |Queries Knowledge| Q
        D --> |Requests Fixes| C
        A --> |Provides Designs| C
        Q --> |Informs Architecture| A
        C --> |Reports Challenges| A
        C --> |Submits Debug Candidates| D
        D --> |Provides Tests| C
        D --> |Alerts Performance| A
        D --> |Recommends Optimizations| C
        D --> |Triggers Anomaly Response| O
    end
```

## Implementation Checklist
1. [ ] Create memory-bank directory structure
2. [ ] Initialize core Markdown files
3. [ ] Implement automatic context propagation
4. [ ] Setup versioned snapshots
5. [ ] Configure validation gateways
6. [ ] Integrate cross-mode coordination
7. [ ] Add anomaly detection patterns
8. [ ] Implement performance profiling hooks
9. [ ] Establish documentation update triggers

## Maintenance Procedures
```mermaid
flowchart TD
    Start[Update Trigger] --> Review[Review ALL Files]
    Review --> Validate[Validate Context Consistency]
    Validate --> Document[Document Changes]
    Document --> Version[Create Snapshot]
    Version --> Notify[Notify Dependent Modes]
```

**Note**: The memory bank must be maintained with precision and clarity as it's the only persistent knowledge store across sessions.