# GriefPreventionPlus
GriefPreventionPlus offers better performances, data integrity, extension capabilities, it currently supports MC1.7.10 and higher (1.8.9, 1.9.4, 1.10.2 and 1.12.2 were tested). 

There's a (old) MC1.6.4 version too (thanks to @CappyT and @dedo1911)

Feedback are needed! If you found an issue, please report it!
Please use the [issues](https://github.com/EverNife/GriefPreventionPlus/issues) page on Github!

[FinalCraft](http://www.finalcraft.com.br)'s servers use this plugin.

## Download
All builds can be found under [releases](https://github.com/EverNife/GriefPreventionPlus/releases).

###Installation
- If you've installed GriefPrevention, remove GriefPrevention jar from plugins folder
- Put GriefPreventionPlus jar into plugins folder

If an existing GriefPrevention database is found, a copy will migrate to GriefPreventionPlus.
Your GriefPrevention database won't be removed: you can rollback to GriefPrevention if you need!

If you're using GriefPrevention file based storage, read this: https://github.com/KaiKikuchi/GriefPreventionPlus/issues/11

###Major features
- Has YMAL DataStorage Support (Uses more RAM, only recommended for servers with more than 3GB)
- Get GriefPrevention's last updates on your MC 1.7.10 server!
- MySQL database is a requirement. Removed file based storage.
- Drastically improved database performances and reduced size: bigger servers will notice it!
- Overall performance improvements
- API improvements: all claims and subdivisions have an unique id
- Less waste of resources (RAM)
- You can use the /claim [range] command to claim land around your position!
- Delete claims by id
- Log entry when a player on a claim is killed by another player
- Trust FakePlayers (add a # before the player name: /trust #[CoFH])
- Entry permission (players can't enter claims without /entrytrust permission)
- Autotrust players and fake players with /autotrust

###Extensions
- [GPP-Cities](https://github.com/KaiKikuchi/GriefPreventionPlus-Cities): be a mayor! make your city and invite your friends to your city!
- [GPP-SkyBlock](https://github.com/KaiKikuchi/GriefPreventionPlus-SkyBlock): let your player make their own island with all the GPP features! Definitely useful for SkyFactory-like servers! (
- [GPP-RealEstate](https://github.com/KaiKikuchi/GPPRealEstate): Sell or rent claims and subclaims!
- [ForgeRestrictor](https://github.com/KaiKikuchi/ForgeRestrictor): Enhanced protection for Cauldron-like servers
- [Dynmap-GPP](https://github.com/KaiKikuchi/Dynmap-GriefPreventionPlus): Add-on for Dynmap that shows claims 

####Notice
- GriefPrevention's extensions don't work with GriefPreventionPlus without some little change on the code. If needed, I will fork most important GriefPrevention's extension to make it work with GriefPreventionPlus! You can ask for it!

##Support my life!
Being a freelance developer is my full-time job. I really enjoy this, so I will keep doing it as long as I receive enough donations!
If you like this plugin and you run it fine on your server, please <a href='http://kaikk.net/mc/#donate'>consider a donation</a>!
This will keep me doing this! Thank you very much!
